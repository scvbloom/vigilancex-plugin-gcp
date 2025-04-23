package gcp

import (
	"context"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"

	"google.golang.org/api/bigtableadmin/v2"
)

//// TABLE DEFINITION

func tableGcpBigtableCluster(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_bigtable_cluster",
		Description: "GCP Bigtable Cluster",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name"}),
			Hydrate:    getBigtableCluster,
		},
		List: &plugin.ListConfig{
			Hydrate: listBigtableClusters,
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "A friendly name that identifies the resource.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromCamel().Transform(lastPathElement),
			},
			{
				Name:        "location",
				Description: "The location where this cluster's nodes and storage reside.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "state",
				Description: "The current state of the cluster.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "serve_nodes",
				Description: "The number of nodes allocated to this cluster.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "storage_type",
				Description: "The type of storage used by this cluster.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "default_storage_type",
				Description: "The default storage type for all tables in the cluster.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "encryption_config",
				Description: "The encryption configuration for this cluster.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "cluster_config",
				Description: "The configuration for this cluster.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "labels",
				Description: "Labels are a flexible and lightweight mechanism for organizing cloud resources into groups that reflect a customer's organizational needs and deployment strategies.",
				Type:        proto.ColumnType_JSON,
			},

			// standard steampipe columns
			{
				Name:        "tags",
				Description: ColumnDescriptionTags,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Labels"),
			},
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(bigtableClusterTurbotData, "Akas"),
			},

			// standard gcp columns
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(bigtableClusterTurbotData, "Project"),
			},
		},
	}
}

//// LIST FUNCTION

func listBigtableClusters(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("listBigtableClusters")

	// Create Service Connection
	service, err := BigtableAdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	// Get project details
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	// List all instances first
	instances, err := service.Projects.Instances.List("projects/" + project).Do()
	if err != nil {
		return nil, err
	}

	// For each instance, list its clusters
	for _, instance := range instances.Instances {
		clusters, err := service.Projects.Instances.Clusters.List(instance.Name).Do()
		if err != nil {
			return nil, err
		}

		for _, cluster := range clusters.Clusters {
			d.StreamListItem(ctx, cluster)
		}
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getBigtableCluster(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("getBigtableCluster")

	// Create Service Connection
	service, err := BigtableAdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	name := d.EqualsQuals["name"].GetStringValue()

	// The name format should be projects/{project}/instances/{instance}/clusters/{cluster}
	parts := strings.Split(name, "/")
	if len(parts) != 6 {
		return nil, nil
	}

	cluster, err := service.Projects.Instances.Clusters.Get(name).Do()
	if err != nil {
		return nil, err
	}

	return cluster, nil
}

//// TRANSFORM FUNCTIONS

func bigtableClusterTurbotData(_ context.Context, d *transform.TransformData) (interface{}, error) {
	cluster := d.HydrateItem.(*bigtableadmin.Cluster)
	param := d.Param.(string)

	// get the resource title
	project := strings.Split(cluster.Name, "/")[1]

	turbotData := map[string]interface{}{
		"Project": project,
		"Akas":    []string{"gcp://bigtableadmin.googleapis.com/" + cluster.Name},
	}

	return turbotData[param], nil
}
