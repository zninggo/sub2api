package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// ============================================================================
// Configuration constants (stored in accounts.extra, zero schema migration)
// ============================================================================

const (
	// AutoSyncModelsEnabledExtraKey is the per-account switch stored in
	// accounts.extra. When true, the periodic runner will probe the account's
	// upstream /v1/models endpoint and overwrite model_mapping with the live
	// model list.
	AutoSyncModelsEnabledExtraKey = "auto_sync_models_enabled"

	// AutoSyncModelsLastSyncAtExtraKey records the last successful sync time.
	AutoSyncModelsLastSyncAtExtraKey = "auto_sync_models_last_sync_at"

	// AutoSyncModelsLastErrorExtraKey records the last sync error message.
	AutoSyncModelsLastErrorExtraKey = "auto_sync_models_last_error"
)

// Global setting key for the sync interval (stored in settings table).
// Declared in domain_constants.go to avoid redeclaration.

// ============================================================================
// Runner constants
// ============================================================================

const (
	opsUpstreamModelSyncJobName = "ops_upstream_model_sync"

	opsUpstreamModelSyncDefaultInterval = 6 * time.Hour
	opsUpstreamModelSyncMinInterval     = 30 * time.Minute
	opsUpstreamModelSyncMaxInterval     = 7 * 24 * time.Hour

	opsUpstreamModelSyncTimeout = 5 * time.Minute

	opsUpstreamModelSyncLeaderLockKey = "ops:upstream_model_sync:leader"
	opsUpstreamModelSyncLeaderLockTTL = 6 * time.Minute

	opsUpstreamModelSyncHeartbeatTimeout = 3 * time.Second

	// Per-account fetch timeout.
	opsUpstreamModelSyncAccountTimeout = 30 * time.Second

	// Maximum accounts to sync in one cycle (to bound cycle duration).
	opsUpstreamModelSyncMaxPerCycle = 50

	// Heartbeat timeout for the ops_job_heartbeats upsert.
	opsUpstreamModelSyncHeartbeatStaleAfter = 2 * opsUpstreamModelSyncLeaderLockTTL
)

var opsUpstreamModelSyncAdvisoryLockID = hashAdvisoryLockID(opsUpstreamModelSyncLeaderLockKey)

// ============================================================================
// Model list → model_mapping conversion
// ============================================================================

// UpstreamModelIDsToMapping converts a list of upstream model IDs into a
// model_mapping (map[string]string) where each model ID maps to itself
// (identity mapping). This is the standard 1:1 sync strategy: the request
// model name equals the upstream model name.
func UpstreamModelIDsToMapping(modelIDs []string) map[string]string {
	mapping := make(map[string]string, len(modelIDs))
	for _, id := range modelIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		mapping[id] = id
	}
	return mapping
}

// ============================================================================
// Manual sync orchestration: fetch → convert → write back model_mapping
// ============================================================================

// UpstreamModelSyncResult describes the outcome of a single account sync.
type UpstreamModelSyncResult struct {
	AccountID   int64    `json:"account_id"`
	AccountName string   `json:"account_name"`
	Platform    string   `json:"platform"`
	ModelCount  int      `json:"model_count"`
	Models      []string `json:"models,omitempty"`
	Synced      bool     `json:"synced"`
	Error       string   `json:"error,omitempty"`
}

// SyncAccountModelMapping fetches the live model list from the account's
// upstream, converts it to a model_mapping (identity mapping), and writes it
// back via UpdateAccount — which internally refreshes the Redis scheduler
// cache. On fetch failure the existing model_mapping is left untouched.
//
// adminService is injected (rather than using the receiver) to avoid a
// circular dependency between AccountTestService and AdminService at
// construction time.
func (s *AccountTestService) SyncAccountModelMapping(
	ctx context.Context,
	account *Account,
	adminService AdminService,
) (*UpstreamModelSyncResult, error) {
	result := &UpstreamModelSyncResult{
		AccountID:   account.ID,
		AccountName: account.Name,
		Platform:    account.Platform,
	}

	// Fetch live model IDs from upstream.
	modelIDs, err := s.FetchUpstreamSupportedModels(ctx, account)
	if err != nil {
		result.Error = err.Error()
		slog.Warn("auto_sync_models_fetch_failed",
			"account_id", account.ID,
			"account_name", account.Name,
			"platform", account.Platform,
			"error", err,
		)
		return result, err
	}

	result.Models = modelIDs
	result.ModelCount = len(modelIDs)

	// Convert to model_mapping (identity mapping).
	newMapping := UpstreamModelIDsToMapping(modelIDs)
	if len(newMapping) == 0 {
		// Upstream returned an empty model list — don't wipe the existing
		// mapping; this is almost certainly an upstream issue, not a real
		// "zero models available" state.
		result.Error = "upstream returned empty model list; skipping update"
		slog.Warn("auto_sync_models_empty_upstream",
			"account_id", account.ID,
			"account_name", account.Name,
		)
		return result, fmt.Errorf("upstream returned empty model list for account %d", account.ID)
	}

	// Write back via UpdateAccount. This merges credentials (preserving
	// sensitive keys like api_key/tokens) while replacing model_mapping,
	// and automatically refreshes the Redis scheduler cache.
	credsUpdate := map[string]any{
		"model_mapping": newMapping,
	}
	_, err = adminService.UpdateAccount(ctx, account.ID, &UpdateAccountInput{
		Credentials: credsUpdate,
	})
	if err != nil {
		result.Error = err.Error()
		slog.Error("auto_sync_models_write_failed",
			"account_id", account.ID,
			"account_name", account.Name,
			"error", err,
		)
		return result, err
	}

	// Record sync metadata in extra.
	extraUpdate := map[string]any{
		AutoSyncModelsLastSyncAtExtraKey:  time.Now().UTC().Format(time.RFC3339),
		AutoSyncModelsLastErrorExtraKey:  "",
	}
	_ = adminService.UpdateAccountExtra(ctx, account.ID, extraUpdate)

	result.Synced = true
	slog.Info("auto_sync_models_success",
		"account_id", account.ID,
		"account_name", account.Name,
		"platform", account.Platform,
		"model_count", len(newMapping),
	)
	return result, nil
}

// ============================================================================
// Periodic runner: OpsUpstreamModelSyncService
// ============================================================================

// OpsUpstreamModelSyncService periodically scans all accounts with
// extra.auto_sync_models_enabled=true and syncs their model_mapping from
// the upstream /v1/models endpoint.
type OpsUpstreamModelSyncService struct {
	opsRepo         OpsRepository
	settingRepo     SettingRepository
	accountRepo     AccountRepository
	accountTestSvc  *AccountTestService
	adminService    AdminService
	cfg             *config.Config
	db              *sql.DB
	redisClient     *redis.Client
	instanceID      string

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}

	skipLogMu  sync.Mutex
	skipLogAt  time.Time
}

// NewOpsUpstreamModelSyncService creates (but does not start) the runner.
func NewOpsUpstreamModelSyncService(
	opsRepo OpsRepository,
	settingRepo SettingRepository,
	accountRepo AccountRepository,
	accountTestSvc *AccountTestService,
	adminService AdminService,
	cfg *config.Config,
	db *sql.DB,
	redisClient *redis.Client,
) *OpsUpstreamModelSyncService {
	return &OpsUpstreamModelSyncService{
		opsRepo:        opsRepo,
		settingRepo:    settingRepo,
		accountRepo:    accountRepo,
		accountTestSvc: accountTestSvc,
		adminService:   adminService,
		cfg:            cfg,
		db:             db,
		redisClient:    redisClient,
		instanceID:     uuid.NewString(),
	}
}

func (s *OpsUpstreamModelSyncService) Start() {
	if s == nil {
		return
	}
	s.startOnce.Do(func() {
		s.stopCh = make(chan struct{})
		go s.run()
	})
}

func (s *OpsUpstreamModelSyncService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.stopCh != nil {
			close(s.stopCh)
		}
	})
}

func (s *OpsUpstreamModelSyncService) run() {
	// First run shortly after startup (give the server time to initialize).
	select {
	case <-time.After(30 * time.Second):
	case <-s.stopCh:
		return
	}

	s.syncOnce()

	for {
		interval := s.getInterval()
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
			s.syncOnce()
		case <-s.stopCh:
			timer.Stop()
			return
		}
	}
}

func (s *OpsUpstreamModelSyncService) getInterval() time.Duration {
	interval := opsUpstreamModelSyncDefaultInterval
	if s.settingRepo == nil {
		return interval
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	raw, err := s.settingRepo.GetValue(ctx, SettingKeyOpsUpstreamModelSyncIntervalSeconds)
	if err != nil {
		return interval
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return interval
	}

	seconds, err := strconv.Atoi(raw)
	if err != nil {
		return interval
	}
	d := time.Duration(seconds) * time.Second
	if d < opsUpstreamModelSyncMinInterval {
		return opsUpstreamModelSyncMinInterval
	}
	if d > opsUpstreamModelSyncMaxInterval {
		return opsUpstreamModelSyncMaxInterval
	}
	return d
}

func (s *OpsUpstreamModelSyncService) syncOnce() {
	if s == nil {
		return
	}
	if s.cfg != nil && !s.cfg.Ops.Enabled {
		return
	}
	if s.opsRepo == nil || s.accountRepo == nil || s.accountTestSvc == nil || s.adminService == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), opsUpstreamModelSyncTimeout)
	defer cancel()

	// Leader lock — only one instance runs the sync per cycle.
	release, ok := s.tryAcquireLeaderLock(ctx)
	if !ok {
		return
	}
	if release != nil {
		defer release()
	}

	startedAt := time.Now().UTC()
	result := s.syncAllAccounts(ctx)
	finishedAt := time.Now().UTC()
	durationMs := finishedAt.Sub(startedAt).Milliseconds()

	// Write heartbeat.
	s.recordHeartbeat(ctx, result, startedAt, durationMs)
}

func (s *OpsUpstreamModelSyncService) syncAllAccounts(ctx context.Context) *upstreamModelSyncCycleResult {
	result := &upstreamModelSyncCycleResult{
		StartedAt: time.Now().UTC(),
	}

	// Fetch all active accounts across all platforms. The list is small
	// (typically < 1000); we filter in-memory for the auto_sync flag.
	// We use ListActive which returns all non-deleted active accounts.
	accounts, err := s.accountRepo.ListActive(ctx)
	if err != nil {
		result.Error = err.Error()
		slog.Error("auto_sync_models_list_failed", "error", err)
		return result
	}

 synced := 0
	failed := 0
	skipped := 0
	for _, account := range accounts {
		if !isAutoSyncModelsEnabled(&account) {
			continue
		}
		if len(result.Details) >= opsUpstreamModelSyncMaxPerCycle {
			skipped++
			continue
		}

		// Per-account timeout to avoid one slow upstream blocking the cycle.
		acctCtx, acctCancel := context.WithTimeout(ctx, opsUpstreamModelSyncAccountTimeout)
		syncResult, syncErr := s.accountTestSvc.SyncAccountModelMapping(acctCtx, &account, s.adminService)
		acctCancel()

		if syncErr != nil {
			failed++
			// Record error in extra.
			_ = s.adminService.UpdateAccountExtra(ctx, account.ID, map[string]any{
				AutoSyncModelsLastErrorExtraKey: truncateString(syncErr.Error(), 500),
			})
		} else if syncResult.Synced {
			synced++
		}

		result.Details = append(result.Details, UpstreamModelSyncResult{
			AccountID:   account.ID,
			AccountName: account.Name,
			Platform:    account.Platform,
			ModelCount:  syncResult.ModelCount,
			Synced:      syncResult.Synced,
			Error:       syncResult.Error,
		})
	}

	result.Synced = synced
	result.Failed = failed
	result.Skipped = skipped
	result.Duration = time.Since(result.StartedAt)
	return result
}

type upstreamModelSyncCycleResult struct {
	StartedAt time.Time
	Duration  time.Duration
	Synced    int
	Failed    int
	Skipped   int
	Details   []UpstreamModelSyncResult
	Error     string
}

func (s *OpsUpstreamModelSyncService) recordHeartbeat(ctx context.Context, result *upstreamModelSyncCycleResult, startedAt time.Time, durationMs int64) {
	if s.opsRepo == nil {
		return
	}

	lastResult := fmt.Sprintf("synced=%d failed=%d skipped=%d duration=%s",
		result.Synced, result.Failed, result.Skipped, result.Duration)

	input := OpsUpsertJobHeartbeatInput{
		JobName:        opsUpstreamModelSyncJobName,
		LastRunAt:      &startedAt,
		LastDurationMs: &durationMs,
		LastResult:     &lastResult,
	}

	if result.Error != "" {
		errMsg := truncateString(result.Error, 500)
		now := time.Now().UTC()
		input.LastErrorAt = &now
		input.LastError = &errMsg
	} else {
		now := time.Now().UTC()
		input.LastSuccessAt = &now
	}

	heartbeatCtx, cancel := context.WithTimeout(ctx, opsUpstreamModelSyncHeartbeatTimeout)
	defer cancel()

	if err := s.opsRepo.UpsertJobHeartbeat(heartbeatCtx, &input); err != nil {
		slog.Warn("auto_sync_models_heartbeat_failed", "error", err)
	}
}

// ============================================================================
// Leader lock (Redis SetNX + PG advisory lock fallback)
// ============================================================================

var opsUpstreamModelSyncReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

func (s *OpsUpstreamModelSyncService) tryAcquireLeaderLock(ctx context.Context) (func(), bool) {
	if s == nil {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Simple mode: no locking, assume single instance.
	if s.cfg != nil && s.cfg.RunMode == config.RunModeSimple {
		return nil, true
	}

	if s.redisClient != nil {
		ok, err := s.redisClient.SetNX(ctx, opsUpstreamModelSyncLeaderLockKey, s.instanceID, opsUpstreamModelSyncLeaderLockTTL).Result()
		if err == nil {
			if !ok {
				s.maybeLogSkip()
				return nil, false
			}
			release := func() {
				ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_, _ = opsUpstreamModelSyncReleaseScript.Run(ctx2, s.redisClient, []string{opsUpstreamModelSyncLeaderLockKey}, s.instanceID).Result()
			}
			return release, true
		}
		// Redis error: fall through to DB advisory lock.
	}

	release, ok := tryAcquireDBAdvisoryLock(ctx, s.db, opsUpstreamModelSyncAdvisoryLockID)
	if !ok {
		s.maybeLogSkip()
		return nil, false
	}
	return release, true
}

func (s *OpsUpstreamModelSyncService) maybeLogSkip() {
	s.skipLogMu.Lock()
	defer s.skipLogMu.Unlock()

	now := time.Now()
	if !s.skipLogAt.IsZero() && now.Sub(s.skipLogAt) < time.Minute {
		return
	}
	s.skipLogAt = now
	slog.Info("ops_upstream_model_sync: leader lock held by another instance; skipping")
}

// ============================================================================
// Helper: check if auto-sync is enabled for an account
// ============================================================================

// isAutoSyncModelsEnabled returns true if the account has
// extra.auto_sync_models_enabled set to true.
func isAutoSyncModelsEnabled(account *Account) bool {
	if account == nil || account.Extra == nil {
		return false
	}
	v, ok := account.Extra[AutoSyncModelsEnabledExtraKey]
	if !ok {
		return false
	}
	enabled, ok := v.(bool)
	return ok && enabled
}

// IsAutoSyncModelsEnabled is the exported wrapper for use in admin service
// (to include the field in account responses).
func IsAutoSyncModelsEnabled(account *Account) bool {
	return isAutoSyncModelsEnabled(account)
}

// ============================================================================
// JSON marshaling helper for cycle result (used in heartbeat last_result)
// ============================================================================

func (r *upstreamModelSyncCycleResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Synced  int                        `json:"synced"`
		Failed  int                        `json:"failed"`
		Skipped int                        `json:"skipped"`
		Detail  []UpstreamModelSyncResult  `json:"details,omitempty"`
	}{
		Synced:  r.Synced,
		Failed:  r.Failed,
		Skipped: r.Skipped,
		Detail:  r.Details,
	})
}
