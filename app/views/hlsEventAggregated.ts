import typia from "typia";
import { MaterializedView, sql } from "@514labs/moose-lib";
import { HLSEventPipeline } from "../ingest/models";

interface HLSEventAggregated {
  eventDate: Date;
  appName: string;
  eventType: string;
  stage: string;
  uniqueSessionsState: string; // AggregateFunction(uniq, String)
  uniqueVisitorsState: string; // AggregateFunction(uniq, String)
  uniquePageviewsState: string; // AggregateFunction(uniq, String)
  totalEventsState: string; // AggregateFunction(count, *)
  uniqueUsersState: string; // AggregateFunction(uniq, String)
  avgBitrateState: string; // AggregateFunction(avg, UInt64)
  maxBitrateState: string; // AggregateFunction(max, UInt64)
  minBitrateState: string; // AggregateFunction(min, UInt64)
  levelSwitchesState: string; // AggregateFunction(countIf, UInt8)
  playbackStartsState: string; // AggregateFunction(countIf, UInt8)
  avgFragmentDurationState: string; // AggregateFunction(avg, Float64)
}

const hlsTable = HLSEventPipeline.table!;
// const hlsColumns = hlsTable.columns;

export const HLSEventAggregatedMV = new MaterializedView<HLSEventAggregated>({
  tableName: "HLSEventAggregated",
  materializedViewName: "HLSEventAggregated_MV",
  orderByFields: ["eventDate", "appName", "eventType"],
  selectStatement: sql`SELECT
    toDate(timestamp) as eventDate,
    appName,
    eventType,
    stage,
    uniqState(sessionId) as uniqueSessionsState,
    uniqState(visitorId) as uniqueVisitorsState,
    uniqState(pageviewId) as uniquePageviewsState,
    countState(*) as totalEventsState,
    uniqState(userId) as uniqueUsersState,
    avgState(bitrate) as avgBitrateState,
    maxState(bitrate) as maxBitrateState,
    minState(bitrate) as minBitrateState,
    countIfState(eventType = 'level-switched') as levelSwitchesState,
    countIfState(eventType = 'playback-started') as playbackStartsState,
    avgState(fragmentDuration) as avgFragmentDurationState
  FROM HLSEvent
  GROUP BY toDate(timestamp), appName, eventType, stage
  `,
  selectTables: [hlsTable],
});
