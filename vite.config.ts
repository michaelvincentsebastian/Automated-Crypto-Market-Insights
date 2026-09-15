import { defineConfig, Plugin } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";
import dotenv from "dotenv";

dotenv.config();

function localApiServerPlugin(): Plugin {
  return {
    name: "local-api-server",
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        const urlStr = req.url || "";
        if (!urlStr.startsWith("/api/")) {
          return next();
        }

        const url = new URL(urlStr, `http://${req.headers.host}`);
        const pathname = url.pathname;

        // Map route to handler file
        let handlerModule: any = null;
        try {
          if (pathname === "/api/dashboard/overview") {
            handlerModule = await server.ssrLoadModule("/api/dashboard/overview.ts");
          } else if (pathname === "/api/dashboard/listings") {
            handlerModule = await server.ssrLoadModule("/api/dashboard/listings.ts");
          } else if (pathname === "/api/dashboard/refresh") {
            handlerModule = await server.ssrLoadModule("/api/dashboard/refresh.ts");
          } else if (pathname === "/api/dashboard/coin") {
            handlerModule = await server.ssrLoadModule("/api/dashboard/coin.ts");
          } else if (pathname === "/api/health") {
            handlerModule = await server.ssrLoadModule("/api/health.ts");
          } else if (pathname === "/api/ingest/global") {
            handlerModule = await server.ssrLoadModule("/api/ingest/global.ts");
          } else if (pathname === "/api/ingest/listings") {
            handlerModule = await server.ssrLoadModule("/api/ingest/listings.ts");
          } else if (pathname === "/api/ingest/feargreed") {
            handlerModule = await server.ssrLoadModule("/api/ingest/feargreed.ts");
          }

          if (handlerModule && typeof handlerModule.default === "function") {
            // Adapt query params
            const query: Record<string, string> = {};
            url.searchParams.forEach((v, k) => {
              query[k] = v;
            });

            // Mock VercelRequest and VercelResponse
            const vercelReq: any = req;
            vercelReq.query = query;

            const vercelRes: any = res;
            vercelRes.status = (statusCode: number) => {
              res.statusCode = statusCode;
              return vercelRes;
            };
            vercelRes.json = (data: any) => {
              res.setHeader("Content-Type", "application/json");
              res.end(JSON.stringify(data));
              return vercelRes;
            };

            await handlerModule.default(vercelReq, vercelRes);
            return;
          }
        } catch (err: any) {
          console.error(`[API Dev Error] ${pathname}:`, err);
          res.statusCode = 500;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ error: err.message || "Internal Server Error" }));
          return;
        }

        next();
      });
    },
  };
}

export default defineConfig({
  plugins: [react(), localApiServerPlugin()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    port: 3000,
    host: true,
  },
});
