package gcp

import (
	"context"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/file/v1"
)

func tableGcpFilestoreInstance(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_filestore_instance",
		Description: "GCP Filestore Instance",
		Get: &plugin.GetConfig{
			KeyColumns:        plugin.SingleColumn("name"),
			Hydrate:           getFilestoreInstance,
			ShouldIgnoreError: isIgnorableError([]string{"NOT_FOUND", "PERMISSION_DENIED"}),
		},
		List: &plugin.ListConfig{
			Hydrate:           listFilestoreInstances,
			ShouldIgnoreError: isIgnorableError([]string{"PERMISSION_DENIED"}),
		},
		GetMatrixItemFunc: BuildFilestoreLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The resource name of the Filestore instance.",
			},
			{
				Name:        "description",
				Type:        proto.ColumnType_STRING,
				Description: "The description of the Filestore instance.",
			},
			{
				Name:        "tier",
				Type:        proto.ColumnType_STRING,
				Description: "The service tier of the Filestore instance.",
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The current state of the Filestore instance.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("CreateTime"),
				Description: "The creation time of the Filestore instance.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "The labels associated with the Filestore instance.",
			},
			{
				Name:        "networks",
				Type:        proto.ColumnType_JSON,
				Description: "The network configurations of the Filestore instance.",
			},
			{
				Name:        "file_shares",
				Type:        proto.ColumnType_JSON,
				Description: "The file share configurations of the Filestore instance.",
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
				Transform:   transform.FromP(gcpFilestoreInstanceTurbotData, "Akas"),
			},
			// Standard GCP columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpFilestoreInstanceTurbotData, "Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Hydrate:     getProject,
				Transform:   transform.FromValue(),
			},
		},
	}
}

//// LIST FUNCTION

func listFilestoreInstances(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	region := d.EqualsQualString("location")
	var location string
	matrixLocation := d.EqualsQualString(matrixKeyLocation)

	// Validate location using BuildLocationList
	if matrixLocation != "" {
		location = matrixLocation
	} else {
		logger.Error("gcp_filestore_instance.listFilestoreInstances", "invalid_location", "Matrix location is empty")
		return nil, nil
	}

	// Minimize API call as per given location
	if region != "" && region != location {
		logger.Warn("gcp_filestore_instance.listFilestoreInstances", "location_mismatch", "region", region, "matrixLocation", location)
		return nil, nil
	}

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_filestore_instance.listFilestoreInstances", "project_error", err)
		return nil, err
	}
	project := projectData.Project

	// Create Service Connection
	service, err := FilestoreService(ctx, d)
	if err != nil {
		logger.Error("gcp_filestore_instance.listFilestoreInstances", "connection_error", err)
		return nil, err
	}
	//loc := d.EqualsQuals["location"].GetStringValue()

	// List Filestore instances
	req := service.Projects.Locations.Instances.List("projects/" + project + "/locations/" + location)
	err = req.Pages(ctx, func(page *file.ListInstancesResponse) error {
		for _, instance := range page.Instances {
			d.StreamListItem(ctx, instance)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_filestore_instance.listFilestoreInstances", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getFilestoreInstance(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	region := d.EqualsQualString("location")
	var location string
	matrixLocation := d.EqualsQualString(matrixKeyLocation)

	// Validate location using BuildLocationList
	if matrixLocation != "" {
		location = matrixLocation
	} else {
		logger.Error("gcp_filestore_instance.getFilestoreInstance", "invalid_location", "Matrix location is empty")
		return nil, nil
	}

	// Minimize API call as per given location
	if region != "" && region != location {
		logger.Warn("gcp_filestore_instance.getFilestoreInstance", "location_mismatch", "region", region, "matrixLocation", location)
		return nil, nil
	}

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_filestore_instance.getFilestoreInstance", "project_error", err)
		return nil, err
	}
	project := projectData.Project

	name := d.EqualsQualString("name")

	// Validate - name should not be blank
	if name == "" {
		logger.Error("gcp_filestore_instance.getFilestoreInstance", "invalid_name", "Name is empty")
		return nil, nil
	}

	// Create Service Connection
	service, err := FilestoreService(ctx, d)
	if err != nil {
		logger.Error("gcp_filestore_instance.getFilestoreInstance", "connection_error", err)
		return nil, err
	}

	//loc := d.EqualsQuals["location"].GetStringValue()

	// Get Filestore instance
	req := service.Projects.Locations.Instances.Get("projects/" + project + "/locations/" + location + "/instances/" + name)
	instance, err := req.Do()
	if err != nil {
		logger.Error("gcp_filestore_instance.getFilestoreInstance", "api_error", err)
		return nil, err
	}

	return instance, nil
}

//// TRANSFORM FUNCTIONS

func gcpFilestoreInstanceTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	instance := d.HydrateItem.(*file.Instance)
	akas := []string{"gcp://file.googleapis.com/" + instance.Name}

	turbotData := map[string]interface{}{
		"Location": strings.Split(instance.Name, "/")[3],
		"Akas":     akas,
	}
	return turbotData[param], nil
}
