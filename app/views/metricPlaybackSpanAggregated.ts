import typia from "typia";
import { MaterializedView, sql } from "@514labs/moose-lib";
import { MetricEventPipeline } from "../ingest/models";

interface PlaybackSpanAggregated {
  eventDate: Date;
  appName: string;
  stage: string;
  videoIdKey: string;
  sessionId: string;
  spans: number & typia.tags.Type<"uint64">;
  avgDurationMs: number & typia.tags.Type<"float">;
}

const metricTable = MetricEventPipeline.table!;

// Aggregates playback-span metric events by date/app/stage/video/session
export const PlaybackSpanAggregatedMV =
  new MaterializedView<PlaybackSpanAggregated>({
    tableName: "PlaybackSpanAggregated",
    materializedViewName: "PlaybackSpanAggregated_MV",
    orderByFields: ["eventDate", "appName", "stage", "videoIdKey", "sessionId"],
    selectStatement: sql`SELECT
    toDate(timestamp) as eventDate,
    appName,
    stage,
    ifNull(videoId, '') as videoIdKey,
    sessionId,
    countIf(eventType = 'playback-span') as spans,
    avgIf(
      toFloat64OrZero(JSON_VALUE(metricDataJson, '$.durationMs')),
      eventType = 'playback-span'
    ) as avgDurationMs
  FROM MetricEvent
  GROUP BY
    toDate(timestamp),
    appName,
    stage,
    ifNull(videoId, ''),
    sessionId
  `,
    selectTables: [metricTable],
  });
