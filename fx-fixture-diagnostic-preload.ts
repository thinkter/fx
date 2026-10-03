import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { TmuxSession } from "./tests/e2e/tmux-helpers";
interface Observation { trace: string; stderr: string | undefined; home: string | undefined; }
const observations = new Map<string, Observation>();
let sequence = 0;
const originalCreate = TmuxSession.create;
TmuxSession.create = async (options) => {
 const trace = options?.env?.FX_TRACE_LOG ?? join(tmpdir(), `fx-fixture-diagnostic-${process.pid}-${++sequence}.log`);
 const session = await originalCreate.call(TmuxSession, {...options, env: {...options?.env, FX_TRACE_LOG: trace, FX_TRACE_SCOPES: `${options?.env?.FX_TRACE_SCOPES ?? ""},input,permission,frame_commit,resize,theme,event_loop`}});
 observations.set(session.name, {trace, stderr: options?.stderrPath, home: options?.env?.HOME});
 console.error("DIAG_CREATE " + JSON.stringify({name: session.name, alive: session.isAlive(), theme: options?.env?.FX_THEME, home: options?.env?.HOME}));
 return session;
};
const originalSendKeys = TmuxSession.prototype.sendKeys;
TmuxSession.prototype.sendKeys = async function(...keys) {
 console.error("DIAG_KEYS " + JSON.stringify({name: this.name, keys}));
 await originalSendKeys.call(this, ...keys);
};
const originalKill = TmuxSession.prototype.kill;
TmuxSession.prototype.kill = async function() {
 const observation = observations.get(this.name);
 console.error("DIAG_PANE " + JSON.stringify({name: this.name, alive: this.isAlive(), pane: await this.capturePane()}));
 if (observation) {
  if (observation.stderr && existsSync(observation.stderr)) console.error("DIAG_STDERR " + readFileSync(observation.stderr, "utf8"));
  if (existsSync(observation.trace)) console.error("DIAG_TRACE_BEGIN " + this.name + "\n" + readFileSync(observation.trace, "utf8") + "\nDIAG_TRACE_END");
 }
 await originalKill.call(this);
};
