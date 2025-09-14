import { Task, Workflow, OlapTable, Key } from "@514labs/moose-lib";
import { PNXEvent } from "../ingest/models";
import { faker } from "@faker-js/faker";
import { randomUUID } from "crypto";

// Data model for OLAP Table
interface PNXWorkflow {
  id: Key<string>;
  success: boolean;
  message: string;
}

// Create OLAP Table - temporarily commented out to isolate build issue
// const workflowTable = new OlapTable<PNXWorkflow>("PNXWorkflow", {
//   orderByFields: ["id"],
// });

export const ingest = new Task<null, void>("ingest", {
  run: async () => {
    for (let i = 0; i < 1000; i++) {
      const pnxEvent: PNXEvent = {
        eventId: randomUUID(),
        appName: "pnxplayer",
        appVersion: faker.system.semver(),
        userId: faker.internet.email(),
        userName: faker.person.fullName(),
        type: Math.random() < 0.5 ? "analytics" : "hls",
        event: Math.random() < 0.5 ? "panel-close" : "level-switched",
        // Analytics-specific fields (randomly added)
        source: Math.random() < 0.3 ? "panel-manager" : undefined,
        href: Math.random() < 0.2 ? faker.internet.url() : undefined,
        // HLS-specific fields (randomly added)
        level:
          Math.random() < 0.3
            ? faker.number.int({ min: 0, max: 5 })
            : undefined,
        videoIndex:
          Math.random() < 0.3
            ? faker.number.int({ min: 0, max: 10 })
            : undefined,
        bitrate:
          Math.random() < 0.3
            ? faker.number.int({ min: 500000, max: 8000000 })
            : undefined,
        resolution:
          Math.random() < 0.3
            ? faker.helpers.arrayElement(["854x480", "1280x720", "1920x1080"])
            : undefined,
        bandwidth:
          Math.random() < 0.3
            ? faker.number.int({ min: 500000, max: 8000000 })
            : undefined,
        fragmentDuration:
          Math.random() < 0.3
            ? faker.number.float({ min: 2, max: 10 })
            : undefined,
        // Request metadata
        requestTimeEpoch: Date.now(),
        domainName: "api.pnxstudios.com",
        stage: "development",
        sourceIp: faker.internet.ip(),
        userAgent: faker.internet.userAgent(),
      };

      try {
        const response = await fetch("http://localhost:4000/ingest/PNXEvent", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(pnxEvent),
        });

        if (!response.ok) {
          console.log(
            `Failed to ingest record ${i}: ${response.status} ${response.statusText}`
          );
          // Insert ingestion result into OLAP table
          // workflowTable.insert([
          //   { id: "1", success: false, message: response.statusText },
          // ]);
        }
      } catch (error) {
        console.log(`Error ingesting record ${i}: ${error}`);
        // workflowTable.insert([
        //   { id: "1", success: false, message: error.message },
        // ]);
      }

      // Add a small delay to avoid overwhelming the server
      if (i % 100 === 0) {
        console.log(`Ingested ${i} records...`);
        // workflowTable.insert([
        //   { id: "1", success: true, message: `Ingested ${i} records` },
        // ]);
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
  },
  retries: 3,
  timeout: "30s",
});

export const workflow = new Workflow("generator", {
  startingTask: ingest,
  retries: 3,
  timeout: "30s",
  // schedule: "@every 5s",
});
