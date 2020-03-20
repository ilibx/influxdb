package tenant

import (
	"context"
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	"go.uber.org/zap"
)

var _ influxdb.BucketService = (*dupReadBucketService)(nil)
var _ influxdb.OrganizationService = (*dupReadOrganizationService)(nil)
var _ influxdb.UserResourceMappingService = (*dupReadUserResourceMappingService)(nil)
var _ influxdb.UserService = (*dupReadUserService)(nil)

// readOnlyStore is a wrapper for kv.Store that ensures that updates are not applied.
type readOnlyStore struct {
	kv.Store
	log *zap.Logger
}

func (r readOnlyStore) Update(context.Context, func(kv.Tx) error) error {
	r.log.Warn("attempted update on read-only store")
	return nil
}

type tenantService struct {
	influxdb.BucketService
	influxdb.OrganizationService
	influxdb.UserResourceMappingService
	influxdb.UserService
	influxdb.PasswordsService
}

// NewReadOnlyTenantService returns a influxdb.TenantService that cannot update the underlying store.
func NewReadOnlyTenantService(log *zap.Logger, store kv.Store) (influxdb.TenantService, error) {
	ro, err := NewStore(readOnlyStore{
		Store: store,
		log:   log,
	})
	if err != nil {
		return nil, err
	}
	return NewService(ro), nil
}

// NewDuplicateReadTenantService returns a tenant service that duplicates the reads of a kv.Service to a tenant.Service.
// It does so by creating a tenant.Service with the given store.
// For consistent results, the given kv.Service should be built on top of the same store.
// The foreseen use case is to compare two service versions, an old one and a new one.
// The resulting influxdb.TenantService:
//  - forwards writes to the old service;
//  - reads from the old one, if no error is encountered, it reads from the new one;
//  - compares the results obtained and logs the difference, if any.
func NewDuplicateReadTenantService(log *zap.Logger, oldSvc *kv.Service, store kv.Store) (influxdb.TenantService, error) {
	// Using a read-only service to further ensure that no update is applied to the underlying store.
	svc, err := NewReadOnlyTenantService(log, store)
	if err != nil {
		return nil, err
	}
	return tenantService{
		BucketService:              NewDuplicateReadBucketService(log, oldSvc, svc),
		OrganizationService:        NewDuplicateReadOrganizationService(log, oldSvc, svc),
		UserResourceMappingService: NewDuplicateReadUserResourceMappingService(log, oldSvc, svc),
		UserService:                NewDuplicateReadUserService(log, oldSvc, svc),
		PasswordsService:           NewDuplicateReadPasswordService(log, oldSvc, svc),
	}, nil
}

type dupReadBucketService struct {
	log *zap.Logger
	old influxdb.BucketService
	new influxdb.BucketService
}

// NewDuplicateReadBucketService returns a service that mirrors the reads for the given services.
// The foreseen use case is to compare two service versions, an old one and a new one.
// It forwards writes to the old service.
// It reads from the old one, if no error is encountered, it reads from the new one.
// It compares the results obtained and logs the difference, if any.
func NewDuplicateReadBucketService(log *zap.Logger, old influxdb.BucketService, new influxdb.BucketService) influxdb.BucketService {
	return dupReadBucketService{log: log, old: old, new: new}
}

func (s dupReadBucketService) FindBucketByID(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
	o, err := s.old.FindBucketByID(ctx, id)
	if err != nil {
		return o, err
	}
	n, err := s.new.FindBucketByID(ctx, id)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindBucketByID"))
	}
	return o, nil
}

func (s dupReadBucketService) FindBucketByName(ctx context.Context, orgID influxdb.ID, name string) (*influxdb.Bucket, error) {
	o, err := s.old.FindBucketByName(ctx, orgID, name)
	if err != nil {
		return o, err
	}
	n, err := s.new.FindBucketByName(ctx, orgID, name)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindBucketByName"))
	}
	return o, nil
}

func (s dupReadBucketService) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
	o, err := s.old.FindBucket(ctx, filter)
	if err != nil {
		return o, err
	}
	n, err := s.new.FindBucket(ctx, filter)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindBucket"))
	}
	return o, nil
}

func (s dupReadBucketService) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	o, no, err := s.old.FindBuckets(ctx, filter, opt...)
	if err != nil {
		return o, no, err
	}
	n, _, err := s.new.FindBuckets(ctx, filter, opt...)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindBuckets"))
	}
	return o, no, nil
}

func (s dupReadBucketService) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	return s.old.CreateBucket(ctx, b)
}

func (s dupReadBucketService) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	return s.old.UpdateBucket(ctx, id, upd)
}

func (s dupReadBucketService) DeleteBucket(ctx context.Context, id influxdb.ID) error {
	return s.old.DeleteBucket(ctx, id)
}

type dupReadOrganizationService struct {
	log *zap.Logger
	old influxdb.OrganizationService
	new influxdb.OrganizationService
}

// NewDuplicateReadOrganizationService returns a service that mirrors the reads for the given services.
// The foreseen use case is to compare two service versions, an old one and a new one.
// It forwards writes to the old service.
// It reads from the old one, if no error is encountered, it reads from the new one.
// It compares the results obtained and logs the difference, if any.
func NewDuplicateReadOrganizationService(log *zap.Logger, old influxdb.OrganizationService, new influxdb.OrganizationService) influxdb.OrganizationService {
	return dupReadOrganizationService{log: log, old: old, new: new}
}

func (s dupReadOrganizationService) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	o, err := s.old.FindOrganizationByID(ctx, id)
	if err != nil {
		return o, err
	}
	n, err := s.new.FindOrganizationByID(ctx, id)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindOrganizationByID"))
	}
	return o, nil
}

func (s dupReadOrganizationService) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	o, err := s.old.FindOrganization(ctx, filter)
	if err != nil {
		return o, err
	}
	n, err := s.new.FindOrganization(ctx, filter)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindOrganization"))
	}
	return o, nil
}

func (s dupReadOrganizationService) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	o, no, err := s.old.FindOrganizations(ctx, filter, opt...)
	if err != nil {
		return o, no, err
	}
	n, _, err := s.new.FindOrganizations(ctx, filter, opt...)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindOrganizations"))
	}
	return o, no, nil
}

func (s dupReadOrganizationService) CreateOrganization(ctx context.Context, b *influxdb.Organization) error {
	return s.old.CreateOrganization(ctx, b)
}

func (s dupReadOrganizationService) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	return s.old.UpdateOrganization(ctx, id, upd)
}

func (s dupReadOrganizationService) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	return s.old.DeleteOrganization(ctx, id)
}

type dupReadUserResourceMappingService struct {
	log *zap.Logger
	old influxdb.UserResourceMappingService
	new influxdb.UserResourceMappingService
}

// NewDuplicateReadUserResourceMappingService returns a service that mirrors the reads for the given services.
// The foreseen use case is to compare two service versions, an old one and a new one.
// It forwards writes to the old service.
// It reads from the old one, if no error is encountered, it reads from the new one.
// It compares the results obtained and logs the difference, if any.
func NewDuplicateReadUserResourceMappingService(log *zap.Logger, old influxdb.UserResourceMappingService, new influxdb.UserResourceMappingService) influxdb.UserResourceMappingService {
	return dupReadUserResourceMappingService{log: log, old: old, new: new}
}

func (s dupReadUserResourceMappingService) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	o, no, err := s.old.FindUserResourceMappings(ctx, filter, opt...)
	if err != nil {
		return o, no, err
	}
	n, _, err := s.new.FindUserResourceMappings(ctx, filter, opt...)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindUserResourceMappings"))
	}
	return o, no, nil
}

func (s dupReadUserResourceMappingService) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	return s.old.CreateUserResourceMapping(ctx, m)
}

func (s dupReadUserResourceMappingService) DeleteUserResourceMapping(ctx context.Context, resourceID, userID influxdb.ID) error {
	return s.old.DeleteUserResourceMapping(ctx, resourceID, userID)
}

type dupReadUserService struct {
	log *zap.Logger
	old influxdb.UserService
	new influxdb.UserService
}

// NewDuplicateReadUserService returns a service that mirrors the reads for the given services.
// The foreseen use case is to compare two service versions, an old one and a new one.
// It forwards writes to the old service.
// It reads from the old one, if no error is encountered, it reads from the new one.
// It compares the results obtained and logs the difference, if any.
func NewDuplicateReadUserService(log *zap.Logger, old influxdb.UserService, new influxdb.UserService) influxdb.UserService {
	return dupReadUserService{log: log, old: old, new: new}
}

func (s dupReadUserService) FindUserByID(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
	o, err := s.old.FindUserByID(ctx, id)
	if err != nil {
		return o, err
	}
	n, err := s.new.FindUserByID(ctx, id)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindUserByID"))
	}
	return o, nil
}

func (s dupReadUserService) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	o, err := s.old.FindUser(ctx, filter)
	if err != nil {
		return o, err
	}
	n, err := s.new.FindUser(ctx, filter)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindUser"))
	}
	return o, nil
}

func (s dupReadUserService) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
	o, no, err := s.old.FindUsers(ctx, filter, opt...)
	if err != nil {
		return o, no, err
	}
	n, _, err := s.new.FindUsers(ctx, filter, opt...)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindUsers"))
	}
	return o, no, nil
}

func (s dupReadUserService) CreateUser(ctx context.Context, u *influxdb.User) error {
	return s.old.CreateUser(ctx, u)
}

func (s dupReadUserService) UpdateUser(ctx context.Context, id influxdb.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	return s.old.UpdateUser(ctx, id, upd)
}

func (s dupReadUserService) DeleteUser(ctx context.Context, id influxdb.ID) error {
	return s.old.DeleteUser(ctx, id)
}

type dupReadPasswordService struct {
	log *zap.Logger
	old influxdb.PasswordsService
	new influxdb.PasswordsService
}

// NewDuplicateReadPasswordService returns a service that mirrors the reads for the given services.
// The foreseen use case is to compare two service versions, an old one and a new one.
// It forwards writes to the old service.
// It reads from the old one, if no error is encountered, it reads from the new one.
// It compares the results obtained and logs the difference, if any.
func NewDuplicateReadPasswordService(log *zap.Logger, old influxdb.PasswordsService, new influxdb.PasswordsService) influxdb.PasswordsService {
	return dupReadPasswordService{log: log, old: old, new: new}
}

func (s dupReadPasswordService) SetPassword(ctx context.Context, userID influxdb.ID, password string) error {
	return s.old.SetPassword(ctx, userID, password)
}

func (s dupReadPasswordService) ComparePassword(ctx context.Context, userID influxdb.ID, password string) error {
	if err := s.old.ComparePassword(ctx, userID, password); err != nil {
		return err
	}
	err := s.new.ComparePassword(ctx, userID, password)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	}
	return nil
}

func (s dupReadPasswordService) CompareAndSetPassword(ctx context.Context, userID influxdb.ID, old, new string) error {
	return s.old.CompareAndSetPassword(ctx, userID, old, new)
}
