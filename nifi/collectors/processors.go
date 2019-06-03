package collectors

import (
	"github.com/prometheus/client_golang/prometheus"
	"nifi_exporter/nifi/client"
)

type ProcessorsCollector struct {
	api *client.Client

	bulletin5mCount *prometheus.Desc

	inFlowFiles5mCount  *prometheus.Desc
	inBytes5mCount      *prometheus.Desc
	readBytes5mCount    *prometheus.Desc
	writtenBytes5mCount *prometheus.Desc
	outFlowFiles5mCount *prometheus.Desc
	outBytes5mCount     *prometheus.Desc
	activeThreadCount   *prometheus.Desc
	taskDurationNanos   *prometheus.Desc
}

func NewProcessorsCollector(api *client.Client, labels map[string]string) *ProcessorsCollector {
	prefix := MetricNamePrefix + "proc_"
	statLabels := []string{"node_id", "processor", "proc_id", "group_id"}
	return &ProcessorsCollector{
		api: api,

		bulletin5mCount: prometheus.NewDesc(
			prefix+"bulletin_5m_count",
			"Number of bulletins posted during last 5 minutes.",
			[]string{"processor", "proc_id", "group_id", "level"},
			labels,
		),

		inFlowFiles5mCount: prometheus.NewDesc(
			prefix+"in_flow_files_5m_count",
			"The number of FlowFiles that have come into this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		inBytes5mCount: prometheus.NewDesc(
			prefix+"in_bytes_5m_count",
			"The number of bytes that have come into this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		readBytes5mCount: prometheus.NewDesc(
			prefix+"read_bytes_5m_count",
			"The number of bytes read by components in this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		writtenBytes5mCount: prometheus.NewDesc(
			prefix+"written_bytes_5m_count",
			"The number of bytes written by components in this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		outFlowFiles5mCount: prometheus.NewDesc(
			prefix+"out_flow_files_5m_count",
			"The number of FlowFiles transferred out of this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),
		outBytes5mCount: prometheus.NewDesc(
			prefix+"out_bytes_5m_count",
			"The number of bytes transferred out of this ProcessGroup in the last 5 minutes",
			statLabels,
			labels,
		),

		activeThreadCount: prometheus.NewDesc(
			prefix+"active_thread_count",
			"The active thread count for this process group.",
			statLabels,
			labels,
		),
		taskDurationNanos: prometheus.NewDesc(
			prefix+"task_duration_5m_nonos",
			"The number of nanoseconds that this Processor has spent running in the last 5 minutes.",
			statLabels,
			labels,
		),
	}
}

func (c *ProcessorsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.bulletin5mCount

	ch <- c.inFlowFiles5mCount
	ch <- c.inBytes5mCount
	ch <- c.readBytes5mCount
	ch <- c.writtenBytes5mCount
	ch <- c.outFlowFiles5mCount
	ch <- c.outBytes5mCount
	ch <- c.activeThreadCount
}

func (c *ProcessorsCollector) Collect(ch chan<- prometheus.Metric) {

	processGroupSlice := make([]client.ProcessGroupEntity, 0)

	processGroups, err := c.api.GetProcessGroups(rootProcessGroupID)
	if err != nil {
		return
	}

	for _, pg := range processGroups {
		processGroupSlice = append(processGroupSlice, pg)
		deepProcessGroups, err := c.api.GetDeepProcessGroups(pg.ID)
		if err != nil {
			return
		}
		for _, dpg := range deepProcessGroups {
			processGroupSlice = append(processGroupSlice, dpg)
		}
	}

	for _, pg := range processGroupSlice {
		entities, err := c.api.GetProcessors(pg.ID)
		if err != nil {
			ch <- prometheus.NewInvalidMetric(c.bulletin5mCount, err)
			return
		}

		for i := range entities {
			c.collect(ch, &entities[i])
		}
	}

}

func (c *ProcessorsCollector) collect(ch chan<- prometheus.Metric, entity *client.ProcessorEntity) {
	bulletinCount := map[string]int{
		"INFO":    0,
		"WARNING": 0,
		"ERROR":   0,
	}
	for i := range entity.Bulletins {
		bulletinCount[entity.Bulletins[i].Bulletin.Level]++
	}
	for level, count := range bulletinCount {
		ch <- prometheus.MustNewConstMetric(
			c.bulletin5mCount,
			prometheus.GaugeValue,
			float64(count),
			entity.Component.Name,
			entity.Component.ID,
			entity.Component.ParentGroupId,
			level,
		)
	}

	nodes := make(map[string]*client.ProcessorStatusSnapshotDTO)
	if len(entity.Status.NodeSnapshots) > 0 {
		for i := range entity.Status.NodeSnapshots {
			snapshot := &entity.Status.NodeSnapshots[i]
			nodes[snapshot.NodeID] = &snapshot.StatusSnapshot
		}
	} else if entity.Status.AggregateSnapshot != nil {
		nodes[AggregateNodeID] = entity.Status.AggregateSnapshot
	}

	for nodeID, snapshot := range nodes {
		ch <- prometheus.MustNewConstMetric(
			c.inFlowFiles5mCount,
			prometheus.GaugeValue,
			float64(snapshot.FlowFilesIn),
			nodeID,
			snapshot.Name,
			snapshot.ID,
			snapshot.GroupID,
		)
		ch <- prometheus.MustNewConstMetric(
			c.inBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesIn),
			nodeID,
			snapshot.Name,
			snapshot.ID,
			snapshot.GroupID,
		)
		ch <- prometheus.MustNewConstMetric(
			c.readBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesRead),
			nodeID,
			snapshot.Name,
			snapshot.ID,
			snapshot.GroupID,
		)
		ch <- prometheus.MustNewConstMetric(
			c.writtenBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesWritten),
			nodeID,
			snapshot.Name,
			snapshot.ID,
			snapshot.GroupID,
		)
		ch <- prometheus.MustNewConstMetric(
			c.outFlowFiles5mCount,
			prometheus.GaugeValue,
			float64(snapshot.FlowFilesOut),
			nodeID,
			snapshot.Name,
			snapshot.ID,
			snapshot.GroupID,
		)
		ch <- prometheus.MustNewConstMetric(
			c.outBytes5mCount,
			prometheus.GaugeValue,
			float64(snapshot.BytesOut),
			nodeID,
			snapshot.Name,
			snapshot.ID,
			snapshot.GroupID,
		)
		ch <- prometheus.MustNewConstMetric(
			c.taskDurationNanos,
			prometheus.GaugeValue,
			float64(snapshot.BytesOut),
			nodeID,
			snapshot.Name,
			snapshot.ID,
			snapshot.GroupID,
		)

	}
}
