import typia from "typia";
import { MaterializedView, sql } from "@514labs/moose-lib";
import { HLSEventPipeline } from "../ingest/models";

interface HLSEventAggregated {
  eventDate: Date;
  appName: string;
  eventType: string;
  stage: string;
  totalEventsState: number & typia.tags.Type<"int64">;
  uniqueUsersState: number & typia.tags.Type<"int64">;
  avgBitrateState: number & typia.tags.Type<"double">;
  maxBitrateState: number & typia.tags.Type<"int64">;
  minBitrateState: number & typia.tags.Type<"int64">;
  levelSwitchesState: number & typia.tags.Type<"int64">;
  playbackStartsState: number & typia.tags.Type<"int64">;
  avgFragmentDurationState: number & typia.tags.Type<"double">;
}

const hlsTable = HLSEventPipeline.table!;
const hlsColumns = hlsTable.columns;

export const HLSEventAggregatedMV = new MaterializedView<HLSEventAggregated>({
  tableName: "HLSEventAggregated",
  materializedViewName: "HLSEventAggregated_MV",
  orderByFields: ["eventDate", "appName", "eventType"],
  selectStatement: sql`SELECT
    toDate(timestamp) as eventDate,
    appName,
    eventType,
    stage,
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
