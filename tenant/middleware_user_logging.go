package tenant

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb"
	"go.uber.org/zap"
)

var _ UserService = (*UserLogger)(nil)

type UserLogger struct {
	logger      *zap.Logger
	userService influxdb.UserService
	pwdService  influxdb.PasswordsService
}

// NewUserLogger returns a logging service middleware for the User Service.
func NewUserLogger(log *zap.Logger, s UserService) *UserLogger {
	return &UserLogger{
		logger:      log,
		userService: s.(influxdb.UserService),
		pwdService:  s.(influxdb.PasswordsService),
	}
}

func (l *UserLogger) CreateUser(ctx context.Context, u *influxdb.User) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Error("failed to create user", zap.Error(err), dur)
			return
		}
		l.logger.Info("user create", dur)
	}(time.Now())
	return l.userService.CreateUser(ctx, u)
}

func (l *UserLogger) FindUserByID(ctx context.Context, id influxdb.ID) (u *influxdb.User, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to find user with ID %v", id)
			l.logger.Error(msg, zap.Error(err), dur)
			return
		}
		l.logger.Info("user find by ID", dur)
	}(time.Now())
	return l.userService.FindUserByID(ctx, id)
}

func (l *UserLogger) FindUser(ctx context.Context, filter influxdb.UserFilter) (u *influxdb.User, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Error("failed to find user matching the given filter", zap.Error(err), dur)
			return
		}
		l.logger.Info("user find", dur)
	}(time.Now())
	return l.userService.FindUser(ctx, filter)
}

func (l *UserLogger) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) (users []*influxdb.User, n int, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Error("failed to find users matching the given filter", zap.Error(err), dur)
			return
		}
		l.logger.Info("users find", dur)
	}(time.Now())
	return l.userService.FindUsers(ctx, filter)
}

func (l *UserLogger) UpdateUser(ctx context.Context, id influxdb.ID, upd influxdb.UserUpdate) (u *influxdb.User, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Error("failed to update user", zap.Error(err), dur)
			return
		}
		l.logger.Info("user update", dur)
	}(time.Now())
	return l.userService.UpdateUser(ctx, id, upd)
}

func (l *UserLogger) DeleteUser(ctx context.Context, id influxdb.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to delete user with ID %v", id)
			l.logger.Error(msg, zap.Error(err), dur)
			return
		}
		l.logger.Info("user create", dur)
	}(time.Now())
	return l.userService.DeleteUser(ctx, id)
}

func (l *UserLogger) SetPassword(ctx context.Context, userID influxdb.ID, password string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to set password for user with ID %v", userID)
			l.logger.Error(msg, zap.Error(err), dur)
			return
		}
		l.logger.Info("set password", dur)
	}(time.Now())
	return l.pwdService.SetPassword(ctx, userID, password)
}

func (l *UserLogger) ComparePassword(ctx context.Context, userID influxdb.ID, password string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to compare password for user with ID %v", userID)
			l.logger.Error(msg, zap.Error(err), dur)
			return
		}
		l.logger.Info("compare password", dur)
	}(time.Now())
	return l.pwdService.ComparePassword(ctx, userID, password)
}

func (l *UserLogger) CompareAndSetPassword(ctx context.Context, userID influxdb.ID, old, new string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to compare and set password for user with ID %v", userID)
			l.logger.Error(msg, zap.Error(err), dur)
			return
		}
		l.logger.Info("compare and set password", dur)
	}(time.Now())
	return l.pwdService.CompareAndSetPassword(ctx, userID, old, new)
}
