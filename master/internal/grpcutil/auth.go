package grpcutil

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/o1egl/paseto"

	// TODO switch to google.golang.org/protobuf/proto/.
	"github.com/golang/protobuf/proto" //nolint: staticcheck
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/determined-ai/determined/master/internal/config"
	"github.com/determined-ai/determined/master/internal/db"
	"github.com/determined-ai/determined/master/internal/rbac/audit"
	"github.com/determined-ai/determined/master/internal/user"
	"github.com/determined-ai/determined/master/pkg/model"
	"github.com/determined-ai/determined/master/pkg/ptrs"
	"github.com/determined-ai/determined/proto/pkg/apiv1"
)

const (
	//nolint:gosec // These are not potential hardcoded credentials.
	gatewayTokenHeader     = "grpcgateway-authorization"
	allocationTokenHeader  = "x-allocation-token"
	allocationTokenHeader2 = "X-Allocation-Token"
	userTokenHeader        = "x-user-token"
	cookieName             = "auth"
)

type (
	userContextKey        struct{}
	userSessionContextKey struct{}
)

var unauthenticatedMethods = map[string]bool{
	"/determined.api.v1.Determined/Login":        true,
	"/determined.api.v1.Determined/GetMaster":    true,
	"/determined.api.v1.Determined/GetTelemetry": true,
}

var (
	// ErrInvalidCredentials notifies that the provided credentials are invalid or missing.
	ErrInvalidCredentials = status.Error(codes.Unauthenticated, "invalid credentials")
	// ErrTokenMissing notifies that the bearer token could not be found.
	ErrTokenMissing = status.Error(codes.Unauthenticated, "token missing")
	// ErrNotActive notifies that the user is not active.
	ErrNotActive = status.Error(codes.PermissionDenied, "user is not active")
	// ErrPermissionDenied notifies that the user does not have permission to access the method.
	ErrPermissionDenied = status.Error(codes.PermissionDenied, "user does not have permission")
)

func allocationSessionByTokenBun(token string) (*model.AllocationSession, error) {
	v2 := paseto.NewV2()

	var session model.AllocationSession
	err := v2.Verify(token, db.GetTokenKeys().PublicKey, &session, nil)
	if err != nil {
		log.WithError(err).Debug("failed to verify allocation_session token")
		return nil, db.ErrNotFound
	}

	err = db.Bun().NewSelect().Model(&session).Where("id = ?", session.ID).Scan(context.Background())
	if errors.Cause(err) == sql.ErrNoRows {
		log.WithField("allocation_sessions.id", session.ID).Debug("allocation_session not found")
		return nil, db.ErrNotFound
	} else if err != nil {
		log.WithError(err).WithField("allocation_sessions.id", session.ID).
			Debug("failed to lookup allocation_session")
		return nil, err
	}

	return &session, nil
}

func getAllocationSessionBun(md metadata.MD) (*model.AllocationSession, error) {
	tokens := []string{}

	keys := make([]string, 0)
	for k := range md {
		keys = append(keys, k)
	}
	// fmt.Println("getAllocationSessionBun", keys)
	// FIXME: the header case sensitivity and grpcmetadata prefix cutoff
	// pick key with allocation in it lowercase
	for _, k := range keys {
		if strings.Contains(strings.ToLower(k), "allocation") {
			tokens = md[k]
			break
		}
	}

	if len(tokens) == 0 {
		return nil, ErrTokenMissing
	}

	token := tokens[0]
	if !strings.HasPrefix(token, "Bearer ") {
		return nil, ErrInvalidCredentials
	}
	token = strings.TrimPrefix(token, "Bearer ")

	switch session, err := allocationSessionByTokenBun(token); err {
	case nil:
		return session, nil
	case db.ErrNotFound:
		return nil, ErrInvalidCredentials
	default:
		return nil, err
	}
}

func GetUserCompat(ctx context.Context, md metadata.MD) (*model.User, *model.UserSession, error) {
	tokens := md[userTokenHeader]
	if len(tokens) == 0 {
		tokens = md[gatewayTokenHeader]
	}
	if len(tokens) == 0 {
		allocationSession, err := getAllocationSessionBun(md)
		if err != nil {
			return nil, nil, err
		}
		if allocationSession.OwnerID == nil {
			return nil, nil, status.Error(codes.InvalidArgument,
				"allocation session has no associated user")
		}
		u, err := user.ByID(ctx, *allocationSession.OwnerID)
		if err != nil {
			return nil, nil, err
		}
		return ptrs.Ptr(u.ToUser()), nil, nil
	}

	token := tokens[0]
	if !strings.HasPrefix(token, "Bearer ") {
		return nil, nil, ErrInvalidCredentials
	}
	token = strings.TrimPrefix(token, "Bearer ")

	var userModel *model.User
	var session *model.UserSession
	var err error
	extConfig := config.GetMasterConfig().InternalConfig.ExternalSessions
	userModel, session, err = user.ByToken(ctx, token, &extConfig)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) ||
			errors.Is(err, db.ErrNotFound) ||
			errors.Is(err, jwt.ErrTokenExpired) {
			return nil, nil, ErrInvalidCredentials
		}
		return nil, nil, err
	}

	if !userModel.Active {
		return nil, nil, ErrPermissionDenied
	}
	return userModel, session, nil
}

// GetUser returns the currently logged in user.
func GetUser(ctx context.Context) (*model.User, *model.UserSession, error) {
	if user, ok := ctx.Value(userContextKey{}).(*model.User); ok {
		if session, ok := ctx.Value(userSessionContextKey{}).(*model.UserSession); ok {
			return user, session, nil // User token cache hit.
		}
		return user, nil, nil // Allocation token cache hit.
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, nil, ErrTokenMissing
	}
	// // lowercase map keys
	// for k, v := range md {
	// 	delete(md, k)
	// 	md[strings.ToLower(k)] = v
	// }
	return GetUserCompat(ctx, md)
}

// GetUserExternalToken returns the external token for the currently logged in user.
func GetUserExternalToken(ctx context.Context) (string, error) {
	if config.GetMasterConfig().InternalConfig.ExternalSessions.JwtKey == "" {
		return "", ErrPermissionDenied
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrTokenMissing
	}
	tokens := md[gatewayTokenHeader]
	if len(tokens) == 0 {
		return "", ErrTokenMissing
	}
	token := tokens[0]
	if !strings.HasPrefix(token, "Bearer ") {
		return "", ErrInvalidCredentials
	}
	return strings.TrimPrefix(token, "Bearer "), nil
}

// Return error if user cannot be authenticated or lacks authorization.
func auth(ctx context.Context, db *db.PgDB, fullMethod string,
	extConfig *model.ExternalSessions,
) (*model.User, *model.UserSession, error) {
	if unauthenticatedMethods[fullMethod] {
		return nil, nil, nil
	}

	return GetUser(ctx)
}

func streamAuthInterceptor(db *db.PgDB,
	extConfig *model.ExternalSessions,
) grpc.StreamServerInterceptor {
	return func(
		srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
	) error {
		// Don't cache the result of the stream auth interceptor because
		// we can't easily modify ss's context and
		// we would have to worry about the user session expiring in the context.
		_, _, err := auth(ss.Context(), db, info.FullMethod, extConfig)
		fields := log.Fields{"endpoint": info.FullMethod}
		wrappedSS := grpc_middleware.WrappedServerStream{
			ServerStream:   ss,
			WrappedContext: context.WithValue(ss.Context(), audit.LogKey{}, fields),
		}
		if err != nil {
			return err
		}

		return handler(srv, &wrappedSS)
	}
}

func unaryAuthInterceptor(db *db.PgDB,
	extConfig *model.ExternalSessions,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		user, session, err := auth(ctx, db, info.FullMethod, extConfig)
		if err != nil {
			return nil, err
		}
		if user != nil {
			ctx = context.WithValue(ctx, userContextKey{}, user)
		}
		if session != nil {
			ctx = context.WithValue(ctx, userSessionContextKey{}, session)
		}

		return handler(ctx, req)
	}
}

func authZInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		fields := log.Fields{"endpoint": info.FullMethod}
		ctx = context.WithValue(ctx, audit.LogKey{}, fields)

		return handler(ctx, req)
	}
}

func userTokenResponse(_ context.Context, w http.ResponseWriter, resp proto.Message) error {
	switch r := resp.(type) {
	case *apiv1.LoginResponse:
		http.SetCookie(w, &http.Cookie{
			Name:    cookieName,
			Value:   r.Token,
			Expires: time.Now().Add(user.SessionDuration),
			Path:    "/",
		})
	case *apiv1.LogoutResponse:
		http.SetCookie(w, &http.Cookie{
			Name:    cookieName,
			Value:   "",
			Expires: time.Unix(0, 0),
		})
	}
	return nil
}
