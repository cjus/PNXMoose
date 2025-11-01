import { Api, MooseCache, joinQueries } from "@514labs/moose-lib";
import { tags } from "typia";

interface ErrorImpactQueryParams {
  days?: number;
  severity?: "error" | "warn" | "info";
  groupBy?: "errorClass" | "fileName" | "browser";
  appName?: string;
  stage?: "production" | "staging" | "development";
  limit?: number & tags.Type<"int32">;
}

interface BrowserBreakdown {
  browserName: string;
  count: number;
  percentage: number;
}

interface ErrorImpactResponse {
  errorClass: string;
  errorMessage: string;
  fileName?: string;
  line?: number;
  column?: number;
  occurrences: number;
  affectedUsers: number;
  affectedSessions: number;
  firstOccurrence: string;
  lastOccurrence: string;
  severity: string;
  impactScore: number;
  browserBreakdown: BrowserBreakdown[];
  urlPath?: string;
}

export const ErrorImpactApi = new Api<
  ErrorImpactQueryParams,
  ErrorImpactResponse[]
>(
  "error-impact",
  async (
    { days = 7, severity, groupBy = "errorClass", appName, stage, limit = 100 },
    { client, sql }
  ) => {
    const cache = await MooseCache.get();
    const cacheKey = `error-impact:${days}:${severity || "all"}:${groupBy}:${
      appName || "all"
    }:${stage || "all"}:${limit}`;

    const cached = await cache.get<ErrorImpactResponse[]>(cacheKey);
    if (cached && Array.isArray(cached) && cached.length > 0) {
      return cached;
    }

    // Build filters
    const filters = [sql`timestamp >= now() - INTERVAL ${days} DAY`];
    if (severity) filters.push(sql`severity = ${severity}`);
    if (appName) filters.push(sql`appName = ${appName}`);
    if (stage) filters.push(sql`stage = ${stage}`);

    const where = joinQueries({
      values: filters,
      separator: " AND ",
      prefix: "WHERE ",
    });

    // Determine grouping columns based on groupBy parameter
    let groupByColumns = "errorClass, errorMessage";
    if (groupBy === "fileName") {
      groupByColumns = "fileName, line, column";
    } else if (groupBy === "browser") {
      groupByColumns = "errorClass, errorMessage, browserName";
    }

    // Simplified query - remove complex BrowserStats CTE for now
    const groupBySql = groupBy === "errorClass" 
      ? sql`errorClass, errorMessage, severity`
      : groupBy === "fileName"
      ? sql`fileName, line, column, severity`
      : sql`errorClass, errorMessage, browserName, severity`;

    const query = sql`
      SELECT
        ifNull(errorClass, '') as errorClass,
        ifNull(errorMessage, '') as errorMessage,
        any(fileName) as fileName,
        if(any(line) > 0, any(line), NULL) as line,
        if(any(column) > 0, any(column), NULL) as column,
        toInt32(count(*)) as occurrences,
        toInt32(uniq(userId)) as affectedUsers,
        toInt32(uniq(sessionId)) as affectedSessions,
        toString(min(timestamp)) as firstOccurrence,
        toString(max(timestamp)) as lastOccurrence,
        ifNull(severity, 'error') as severity,
        toInt64(count(*) * uniq(userId)) as impactScore,
        any(urlPath) as urlPath
      FROM ErrorEvent
      ${where}
      GROUP BY ${groupBySql}
      ORDER BY impactScore DESC
      LIMIT ${limit}
    `;

    try {
      const data = await client.query.execute<any>(query);
      const result: any[] = await data.json();

      // Process browser breakdown (simplified - ClickHouse array handling)
      const convertedResult = result.map((item) => {
        // For browser breakdown, we'll query separately if needed
        // For now, return empty array
        const browserBreakdown: BrowserBreakdown[] = [];

        return {
          errorClass: item.errorClass || "",
          errorMessage: item.errorMessage || "",
          fileName: item.fileName || undefined,
          line: item.line ? parseInt(item.line.toString()) : undefined,
          column: item.column ? parseInt(item.column.toString()) : undefined,
          occurrences: parseInt(item.occurrences.toString()) || 0,
          affectedUsers: parseInt(item.affectedUsers.toString()) || 0,
          affectedSessions: parseInt(item.affectedSessions.toString()) || 0,
          firstOccurrence: item.firstOccurrence || "",
          lastOccurrence: item.lastOccurrence || "",
          severity: item.severity || "error",
          impactScore: parseInt(item.impactScore.toString()) || 0,
          browserBreakdown,
          urlPath: item.urlPath || undefined,
        } as ErrorImpactResponse;
      });

      // Cache for 15 minutes
      await cache.set(cacheKey, convertedResult, 900);

      return convertedResult;
    } catch (error) {
      console.error("Error Impact API error:", error);
      return [];
    }
  }
);

