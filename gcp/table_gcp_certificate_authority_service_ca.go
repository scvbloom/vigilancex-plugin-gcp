package gcp

import (
	"context"
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/privateca/v1"
)

func tableGcpCertificateAuthorityServiceCa(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_certificate_authority_service_ca",
		Description: "GCP Certificate Authority Service Certificate Authorities.",
		List: &plugin.ListConfig{
			Hydrate: listCertificateAuthoritiesCA,
		},
		GetMatrixItemFunc: BuildCertificateAuthorityServiceLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The resource name for this Certificate Authority.",
			},
			{
				Name:        "type",
				Type:        proto.ColumnType_STRING,
				Description: "The type of this Certificate Authority.",
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The state of this Certificate Authority.",
			},
			{
				Name:        "tier",
				Type:        proto.ColumnType_STRING,
				Description: "The tier of this Certificate Authority.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time at which this Certificate Authority was created.",
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time at which this Certificate Authority was last updated.",
			},
			{
				Name:        "delete_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time at which this Certificate Authority was deleted.",
			},
			{
				Name:        "expire_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time at which this Certificate Authority will expire.",
			},
			{
				Name:        "pem_ca_certificates",
				Type:        proto.ColumnType_JSON,
				Description: "The PEM-encoded CA certificates that form the CA chain.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Labels associated with this Certificate Authority.",
			},
			{
				Name:        "gcs_bucket",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the Cloud Storage bucket where this Certificate Authority's certificates and CRLs are stored.",
			},
			{
				Name:        "location",
				Type:        proto.ColumnType_STRING,
				Description: "The location of the Certificate Authority.",
				Transform:   transform.FromField("Location"),
			},
			// Standard Steampipe columns
			{
				Name:        "title",
				Description: "Title of the resource.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "akas",
				Description: "Array of globally unique identifier strings (AKAs).",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(gcpCertificateAuthorityServiceCaTurbotData, "Akas"),
			},
		},
	}
}

//// LIST FUNCTION

func listCertificateAuthoritiesCA(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_certificate_authority_service_ca.listCertificateAuthorities", "project_error", err)
		return nil, err
	}
	project := projectData.Project

	// Get location from matrix or use a default location
	matrixLocation := plugin.GetMatrixItem(ctx)[matrixKeyLocation]
	location, ok := matrixLocation.(string)
	if !ok || location == "" {
		location = "us-central1" // Default location
	}

	// Create Service Connection
	service, err := PrivateCAService(ctx, d)
	if err != nil {
		logger.Error("gcp_certificate_authority_service_ca.listCertificateAuthorities", "connection_error", err)
		return nil, err
	}

	// List CA Pools
	caPoolsParent := fmt.Sprintf("projects/%s/locations/%s", project, location)
	caPoolsResp, err := service.Projects.Locations.CaPools.List(caPoolsParent).Do()
	if err != nil {
		logger.Error("gcp_certificate_authority_service_ca.listCertificateAuthorities", "ca_pools_error", err)
		return nil, err
	}

	// Iterate over CA Pools and fetch Certificate Authorities
	for _, caPool := range caPoolsResp.CaPools {
		caParent := caPool.Name
		req := service.Projects.Locations.CaPools.CertificateAuthorities.List(caParent)
		err = req.Pages(ctx, func(page *privateca.ListCertificateAuthoritiesResponse) error {
			for _, ca := range page.CertificateAuthorities {
				d.StreamListItem(ctx, ca)

				// Check if context has been cancelled or if the limit has been hit
				if d.RowsRemaining(ctx) == 0 {
					return nil
				}
			}
			return nil
		})
		if err != nil {
			logger.Error("gcp_certificate_authority_service_ca.listCertificateAuthorities", "certificate_authorities_error", err)
			return nil, err
		}
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpCertificateAuthorityServiceCaTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	ca := d.HydrateItem.(*privateca.CertificateAuthority)

	akas := []string{"gcp://privateca.googleapis.com/" + ca.Name}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
