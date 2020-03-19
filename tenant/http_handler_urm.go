package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb"
	kithttp "github.com/influxdata/influxdb/kit/transport/http"
	"go.uber.org/zap"
)

type urmHandler struct {
	log     *zap.Logger
	svc     influxdb.UserResourceMappingService
	userSvc influxdb.UserService
	api     *kithttp.API

	rt          influxdb.ResourceType
	idLookupKey string
}

// NewURMHandler generates a mountable handler for URMs. It needs to know how it will be looking up your resource id
// this system assumes you are using chi syntax for query string params `/orgs/{id}/` so it can use chi.URLParam().
func NewURMHandler(log *zap.Logger, rt influxdb.ResourceType, idLookupKey string, uSvc influxdb.UserService, urmSvc influxdb.UserResourceMappingService) http.Handler {
	h := &urmHandler{
		log:     log,
		svc:     urmSvc,
		userSvc: uSvc,
		api:     kithttp.NewAPI(kithttp.WithLog(log)),

		rt:          rt,
		idLookupKey: idLookupKey,
	}

	r := chi.NewRouter()
	r.Get("/members", h.getURMsByType(influxdb.Member))
	r.Get("/owners", h.getURMsByType(influxdb.Owner))
	r.Post("/members", h.postURMByType(influxdb.Member))
	r.Post("/owners", h.postURMByType(influxdb.Owner))
	r.Delete("/members/{userID}", h.deleteURM)
	r.Delete("/owners/{userID}", h.deleteURM)
	return r
}

func (h *urmHandler) getURMsByType(userType influxdb.UserType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req, err := h.decodeGetRequest(ctx, r)
		if err != nil {
			h.api.Err(w, err)
			return
		}

		filter := influxdb.UserResourceMappingFilter{
			ResourceID:   req.ResourceID,
			ResourceType: h.rt,
			UserType:     userType,
		}
		mappings, _, err := h.svc.FindUserResourceMappings(ctx, filter)
		if err != nil {
			h.api.Err(w, err)
			return
		}

		users := make([]*influxdb.User, 0, len(mappings))
		for _, m := range mappings {
			if m.MappingType == influxdb.OrgMappingType {
				continue
			}
			user, err := h.userSvc.FindUserByID(ctx, m.UserID)
			if err != nil {
				h.api.Err(w, err)
				return
			}

			users = append(users, user)
		}
		h.log.Debug("Members/owners retrieved", zap.String("users", fmt.Sprint(users)))

		h.api.Respond(w, http.StatusOK, users)
	}
}

type getRequest struct {
	ResourceID influxdb.ID
}

func (h *urmHandler) decodeGetRequest(ctx context.Context, r *http.Request) (*getRequest, error) {
	id := chi.URLParam(r, h.idLookupKey)
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &getRequest{
		ResourceID: i,
	}

	return req, nil
}

func (h *urmHandler) postURMByType(userType influxdb.UserType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req, err := h.decodePostRequest(ctx, r)
		if err != nil {
			h.api.Err(w, err)
			return
		}

		mapping := &influxdb.UserResourceMapping{
			ResourceID:   req.ResourceID,
			ResourceType: h.rt,
			UserID:       req.UserID,
			UserType:     userType,
		}
		if err := h.svc.CreateUserResourceMapping(ctx, mapping); err != nil {
			h.api.Err(w, err)
			return
		}
		h.log.Debug("Member/owner created", zap.String("mapping", fmt.Sprint(mapping)))

		w.WriteHeader(http.StatusNoContent)
	}
}

type postRequest struct {
	UserID     influxdb.ID
	ResourceID influxdb.ID
}

func (h urmHandler) decodePostRequest(ctx context.Context, r *http.Request) (*postRequest, error) {
	id := chi.URLParam(r, h.idLookupKey)
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var rid influxdb.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	u := &influxdb.User{}
	if err := json.NewDecoder(r.Body).Decode(u); err != nil {
		return nil, err
	}

	if !u.ID.Valid() {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "user id missing or invalid",
		}
	}

	return &postRequest{
		UserID:     u.ID,
		ResourceID: rid,
	}, nil
}

func (h *urmHandler) deleteURM(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := h.decodeDeleteRequest(ctx, r)
	if err != nil {
		h.api.Err(w, err)
		return
	}

	if err := h.svc.DeleteUserResourceMapping(ctx, req.resourceID, req.userID); err != nil {
		h.api.Err(w, err)
		return
	}
	h.log.Debug("Member deleted", zap.String("resourceID", req.resourceID.String()), zap.String("memberID", req.userID.String()))

	w.WriteHeader(http.StatusNoContent)
}

type deleteRequest struct {
	userID     influxdb.ID
	resourceID influxdb.ID
}

func (h *urmHandler) decodeDeleteRequest(ctx context.Context, r *http.Request) (*deleteRequest, error) {
	id := chi.URLParam(r, h.idLookupKey)
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var rid influxdb.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	id = chi.URLParam(r, "userID")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing member id",
		}
	}

	var uid influxdb.ID
	if err := uid.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteRequest{
		userID:     uid,
		resourceID: rid,
	}, nil
}
