package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/privateca/v1"
)

func tableGcpCertificateAuthorityService(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_certificate_authority_service",
		Description: "GCP Certificate Authority Service.",
		List: &plugin.ListConfig{
			Hydrate: listCertificateAuthorities,
		},
		GetMatrixItemFunc: BuildCertificateAuthorityServiceLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the Certificate Authority.",
			},
			{
				Name:        "type",
				Type:        proto.ColumnType_STRING,
				Description: "The type of the Certificate Authority.",
			},
			{
				Name:        "lifetime",
				Type:        proto.ColumnType_STRING,
				Description: "The lifetime of the Certificate Authority.",
			},
			{
				Name:        "tier",
				Type:        proto.ColumnType_STRING,
				Description: "The tier of the Certificate Authority.",
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The state of the Certificate Authority.",
			},
			{
				Name:        "gcs_bucket",
				Type:        proto.ColumnType_STRING,
				Description: "The GCS bucket associated with the Certificate Authority.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the Certificate Authority was created.",
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the Certificate Authority was last updated.",
			},
			{
				Name:        "delete_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the Certificate Authority was deleted.",
			},
			{
				Name:        "expire_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time when the Certificate Authority will expire.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Labels associated with the Certificate Authority.",
			},
			{
				Name:        "pem_ca_certificates",
				Type:        proto.ColumnType_JSON,
				Description: "The PEM-encoded CA certificates.",
			},
			{
				Name:        "satisfies_pzs",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the Certificate Authority satisfies physical zone separation (PZS).",
			},
			{
				Name:        "satisfies_pzi",
				Type:        proto.ColumnType_BOOL,
				Description: "Indicates whether the Certificate Authority satisfies physical zone isolation (PZI).",
			},
			// Standard Steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "tags",
				Description: ColumnDescriptionTags,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Labels"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(gcpCertificateAuthorityServiceTurbotData, "Akas"),
			},
		},
	}
}

//// LIST FUNCTION

func listCertificateAuthorities(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_certificate_authority_service.listCertificateAuthorities", "project_error", err)
		return nil, err
	}
	project := projectData.Project

	// Get location from matrix or default to "us-central1"
	location := d.EqualsQualString("location")
	if location == "" {
		matrixLocation := d.EqualsQualString(matrixKeyLocation)
		if matrixLocation != "" {
			location = matrixLocation
		} else {
			location = "global" // Default location
		}
	}

	// Fetch the list of CA pools
	service, err := PrivateCAService(ctx, d)
	if err != nil {
		logger.Error("gcp_certificate_authority_service.listCertificateAuthorities", "connection_error", err)
		return nil, err
	}

	caPoolsResp, err := service.Projects.Locations.CaPools.List("projects/" + project + "/locations/" + location).Do()
	if err != nil {
		logger.Error("gcp_certificate_authority_service.listCertificateAuthorities", "ca_pools_error", err)
		return nil, err
	}

	// Iterate through CA pools and fetch certificate authorities
	for _, caPool := range caPoolsResp.CaPools {
		parent := caPool.Name

		// Log the parent for debugging
		logger.Debug("gcp_certificate_authority_service.listCertificateAuthorities", "parent", parent)

		// List certificate authorities
		req := service.Projects.Locations.CaPools.CertificateAuthorities.List(parent)
		err = req.Pages(ctx, func(page *privateca.ListCertificateAuthoritiesResponse) error {
			for _, certificateAuthority := range page.CertificateAuthorities {
				d.StreamListItem(ctx, certificateAuthority)

				// Check if context has been cancelled or if the limit has been hit
				if d.RowsRemaining(ctx) == 0 {
					return nil
				}
			}
			return nil
		})
		if err != nil {
			logger.Error("gcp_certificate_authority_service.listCertificateAuthorities", "api_error", err)
			return nil, err
		}
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpCertificateAuthorityServiceTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	certificateAuthority := d.HydrateItem.(*privateca.CertificateAuthority)
	akas := []string{"gcp://privateca.googleapis.com/" + certificateAuthority.Name}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
