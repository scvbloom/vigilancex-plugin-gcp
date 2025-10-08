package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/cloudtrace/v1"
)

func tableGcpCloudTraceTraces(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_cloudtrace_traces",
		Description: "GCP Cloud Trace Traces.",
		List: &plugin.ListConfig{
			Hydrate: listCloudTraceTraces,
		},
		//GetMatrixItemFunc: BuildCloudBuildLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "trace_id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique identifier for the trace.",
			},
			{
				Name:        "span_id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique identifier for the span.",
				Transform:   transform.FromField("Spans.SpanId"),
			},
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the span.",
				Transform:   transform.FromField("Spans.Name"),
			},
			{
				Name:        "kind",
				Type:        proto.ColumnType_STRING,
				Description: "The kind of span (e.g., SERVER, CLIENT).",
				Transform:   transform.FromField("Spans.Kind"),
			},
			{
				Name:        "start_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The start time of the span.",
				Transform:   transform.FromField("Spans.StartTime"),
			},
			{
				Name:        "end_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The end time of the span.",
				Transform:   transform.FromField("Spans.EndTime"),
			},
			{
				Name:        "parent_span_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the parent span, if applicable.",
				Transform:   transform.FromField("Spans.ParentSpanId"),
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Labels associated with the span.",
				Transform:   transform.FromField("Spans.Labels"),
			},
			{
				Name:        "project_id",
				Type:        proto.ColumnType_STRING,
				Description: "The project ID associated with the trace.",
			},
			// Standard Steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("TraceId"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(gcpCloudTraceTracesTurbotData, "Akas"),
			},
		},
	}
}

//// LIST FUNCTION

func listCloudTraceTraces(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		logger.Error("gcp_cloudtrace_traces.listCloudTraceTraces", "project_error", err)
		return nil, err
	}
	projectId := projectData.Project

	// Log the project ID for debugging
	logger.Debug("gcp_cloudtrace_traces.listCloudTraceTraces", "project_id", projectId)

	// Create Service Connection
	service, err := CloudTraceService(ctx, d)
	if err != nil {
		logger.Error("gcp_cloudtrace_traces.listCloudTraceTraces", "connection_error", err)
		return nil, err
	}

	// List traces
	req := service.Projects.Traces.List(projectId)
	err = req.Pages(ctx, func(page *cloudtrace.ListTracesResponse) error {
		for _, trace := range page.Traces {
			d.StreamListItem(ctx, trace)

			// Check if context has been cancelled or if the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("gcp_cloudtrace_traces.listCloudTraceTraces", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpCloudTraceTracesTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	param := d.Param.(string)
	trace := d.HydrateItem.(*cloudtrace.Trace)
	akas := []string{"gcp://cloudtrace.googleapis.com/projects/" + trace.ProjectId + "/traces/" + trace.TraceId}

	turbotData := map[string]interface{}{
		"Akas": akas,
	}
	return turbotData[param], nil
}
