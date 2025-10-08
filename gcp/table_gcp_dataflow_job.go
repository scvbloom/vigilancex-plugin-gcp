package gcp

import (
	"context"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	dataflow "google.golang.org/api/dataflow/v1b3"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

//// TABLE DEFINITION

func tableGcpDataflowJob(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_dataflow_job",
		Description: "GCP Dataflow Job",
		List: &plugin.ListConfig{
			Hydrate: listDataflowJobs,
		},
		// Region names follow a standard convention based on Compute Engine region names
		GetMatrixItemFunc: BuildComputeLocationList,
		Columns: []*plugin.Column{
			{ 
				Name: "id",
				Type: proto.ColumnType_STRING,
				Description: "The unique ID of this job.",
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "User-settable and human-readable display name for the job.",
				Transform:   transform.FromField("Name").Transform(getDataflowJobDisplayName),
			},
			{ 
				Name: "project_id",
				Type: proto.ColumnType_STRING,
				Description: "The ID of the Google Cloud project that the job belongs to.",
			},
			{ 
				Name: "name",
				Type: proto.ColumnType_STRING,
				Description: "Optional. The user-specified Dataflow job name.",
			},
			{ 
				Name: "type",
				Type: proto.ColumnType_STRING,
				Description: "Optional. The type of Dataflow job.",
			},
			{ 
				Name: "environment",
				Type: proto.ColumnType_JSON,
				Description: "Optional. The environment for the job.",
			},
			{ 
				Name: "steps",
				Type: proto.ColumnType_JSON,
				Description: "Exactly one of step or stepsLocation should be specified.",
			},
			{ 
				Name: "steps_location",
				Type: proto.ColumnType_STRING,
				Description: "The Cloud Storage location where the steps are stored.",
			},
			{ 
				Name: "current_state",
				Type: proto.ColumnType_STRING,
				Description: "The current state of the job.",
			},
			{ 
				Name: "current_state_time",
				Type: proto.ColumnType_TIMESTAMP,
				Description: "The timestamp associated with the current state.",
			},
			{ 
				Name: "requested_state",
				Type: proto.ColumnType_STRING,
				Description: "The job's requested state. Applies to jobs.update requests.",
			},
			{ 
				Name: "execution_info",
				Type: proto.ColumnType_JSON,
				Description: "Deprecated.",
			},
			{ 
				Name: "create_time",
				Type: proto.ColumnType_TIMESTAMP,
				Description: "The timestamp when the job was initially created.",
			},
			{ 
				Name: "replace_job_id",
				Type: proto.ColumnType_STRING,
				Description: "If this job is an update of an existing job, this field is the job ID of the job it replaced.",
			},
			{ 
				Name: "transform_name_mapping",
				Type: proto.ColumnType_JSON,
				Description: "Optional. The map of transform name prefixes of the job to be replaced to the corresponding name prefixes of the new job.",
			},
			{ 
				Name: "client_request_id",
				Type: proto.ColumnType_STRING,
				Description: "The client's unique identifier of the job, re-used across retried attempts.",
			},
			{ 
				Name: "replaced_by_job_id",
				Type: proto.ColumnType_STRING,
				Description: "If another job is an update of this job (and thus, this job is in JOB_STATE_UPDATED), this field contains the ID of that job.",
			},
			{ 
				Name: "temp_files",
				Type: proto.ColumnType_JSON,
				Description: "A set of files the system should be aware of that are used for temporary storage.",
			},
			{ 
				Name: "labels",
				Type: proto.ColumnType_JSON,
				Description: "User-defined labels for this job.",
			},
			{ 
				Name: "location",
				Type: proto.ColumnType_STRING,
				Description: "Optional. The regional endpoint that contains this job.",
			},
			{ 
				Name: "pipeline_description",
				Type: proto.ColumnType_JSON,
				Description: "Preliminary field: The format of this data may change at any time.",
			},
			{ 
				Name: "stage_states",
				Type: proto.ColumnType_JSON,
				Description: "This field may be mutated by the Cloud Dataflow service; callers cannot mutate it.",
			},
			{ 
				Name: "job_metadata",
				Type: proto.ColumnType_JSON,
				Description: "This field is populated by the Dataflow service to support filtering jobs by the metadata values provided here.",
			},
			{ 
				Name: "start_time",
				Type: proto.ColumnType_TIMESTAMP,
				Description: "The timestamp when the job was started (transitioned to JOB_STATE_PENDING).",
			},
			{ 
				Name: "created_from_snapshot_id",
				Type: proto.ColumnType_STRING,
				Description: "If this is specified, the job's initial state is populated from the given snapshot.",
			},
			{ 
				Name: "satisfies_pzs",
				Type: proto.ColumnType_BOOL,
				Description: "Reserved for future use.",
			},
			{ 
				Name: "runtime_updatable_params",
				Type: proto.ColumnType_JSON,
				Description: "This field may ONLY be modified at runtime using the projects.jobs.update method to adjust job behavior.",
			},
			{ 
				Name: "satisfies_pzi",
				Type: proto.ColumnType_BOOL,
				Description: "Reserved for future use.",
			},
			{ 
				Name: "service_resources",
				Type: proto.ColumnType_JSON,
				Description: "Resources used by the Dataflow Service to run the job.",
			},

			// Standard Steampipe column
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(getDataflowJobDisplayName),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Hydrate:     gcpDataflowJobTurbotData,
				Transform:   transform.FromField("Akas"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Hydrate:     gcpDataflowJobTurbotData,
				Transform:   transform.FromField("Project"),
			},
		},
	}
}

//// FETCH FUNCTIONS

func listDataflowJobs(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)
	// Get project details
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_dataflow.listDataflowJobs", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	// Page size should be in range of [0, 300].
	pageSize := types.Int64(300)
	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < *pageSize {
			pageSize = limit
		}
	}

	// Get location from matrix or default to "us-central1"
	location := d.EqualsQualString("location")
	if location == "" {
		matrixLocation := d.EqualsQualString(matrixKeyLocation)
		if matrixLocation != "" {
			location = matrixLocation
		} else {
			location = "us-central1" // Default location
		}
	}
	location = "asia"

	// Create Service Connection
	service, err := DataflowService(ctx, d)
	if err != nil {
		logger.Error("gcp_dataflow.listDataflowJobs", "service_error", err)
		return nil, err
	}

	// NOTE: Key is a global resource; hence the only supported value for location is `global`.
	resp := service.Projects.Locations.Jobs.List(project , location).PageSize(*pageSize)

	if err := resp.Pages(
		ctx,
		func(page *dataflow.ListJobsResponse) error {
			for _, item := range page.Jobs {
				d.StreamListItem(ctx, item)

				// Check if context has been cancelled or if the limit has been hit (if specified)
				// if there is a limit, it will return the number of rows required to reach this limit
				if d.RowsRemaining(ctx) == 0 {
					page.NextPageToken = ""
					return nil
				}
			}
			return nil
		},
	); err != nil {
		logger.Error("gcp_dataflow.listDataflowJobs", "api_error", err)
		return nil, err
	}

	return nil, nil
}

/// TRANSFORM FUNCTIONS

func gcpDataflowJobTurbotData(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	job := h.Item.(*dataflow.Job)
	akas := []string{"gcp://dataflow.googleapis.com/" + job.Name}
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}

	var location string
	matrixLocation := d.EqualsQualString(matrixKeyLocation)
	// Since, when the service API is disabled, matrixLocation value will be nil
	if matrixLocation != "" {
		location = matrixLocation
	}
	turbotData := map[string]interface{}{
		"Project":  projectId,
		"Location": location,
		"Akas":     akas,
	}
	return turbotData, nil
}

//// TRANSFORM FUNCTION

func getDataflowJobDisplayName(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	displayName := ""
	if h.HydrateItem != nil {
		data := h.HydrateItem.(*dataflow.Job)
		displayName = strings.Split(data.Name, "/")[5]
	}

	return displayName, nil
}