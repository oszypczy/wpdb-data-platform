export const meta = {
  name: 'build-stage',
  description: 'Build one pipeline stage: owner agent implements, qa-engineer verifies independently, up to two fix rounds',
  whenToUse: 'Run with args {stage, owner, docs, scope} for each stage of docs/design/00-overview.md "Plan prac"',
  phases: [
    { title: 'Build', detail: 'component owner implements its design doc' },
    { title: 'Verify', detail: 'qa-engineer turns the "Testy" table into pytest and runs it' },
    { title: 'Fix', detail: 'owner fixes reported failures, QA re-runs' },
  ],
}

// args example:
// { "stage": 2, "owner": "database-cdc",
//   "docs": ["docs/design/02-postgresql.md", "docs/design/03-debezium.md", "docs/design/04-kafka.md"],
//   "scope": "Postgres init, Debezium connector + Connect image, topics as code, AVRO consumer",
//   "levels": ["unit", "integration"] }
const { stage, owner, docs, scope, levels = ['unit', 'integration'] } = args
const MAX_FIX_ROUNDS = 2

const REPORT = {
  type: 'object',
  properties: {
    summary: { type: 'string' },
    changed_files: { type: 'array', items: { type: 'string' } },
    doc_updates: { type: 'array', items: { type: 'string' }, description: 'design doc items resolved or changed' },
    open_items: { type: 'array', items: { type: 'string' } },
  },
  required: ['summary', 'changed_files', 'open_items'],
}
const VERDICT = {
  type: 'object',
  properties: {
    passed: { type: 'boolean' },
    counts: { type: 'object', properties: { passed: { type: 'number' }, failed: { type: 'number' }, skipped: { type: 'number' } } },
    failures: { type: 'array', items: { type: 'object', properties: {
      test_id: { type: 'string' }, claim: { type: 'string' }, expected: { type: 'string' }, observed: { type: 'string' }, evidence: { type: 'string' },
    }, required: ['test_id', 'claim', 'expected', 'observed'] } },
    untested_claims: { type: 'array', items: { type: 'string' } },
    report: { type: 'string' },
  },
  required: ['passed', 'failures', 'untested_claims', 'report'],
}

const docList = docs.join(', ')
phase('Build')
log(`stage ${stage}: ${owner} builds ${scope}`)
let build = await agent(
  `Implement stage ${stage} of the WPDB platform: ${scope}.
Contract: ${docList}. Follow the design-contract skill: resolve every "Do weryfikacji" item and write the answer into the doc.
Deliver working code plus the fixtures qa-engineer will need. Do not write the test suite yourself beyond unit tests of pure logic.
Return: summary, changed files, doc updates, open items.`,
  { agentType: owner, label: `build:${stage}`, phase: 'Build', schema: REPORT },
)
if (!build) return { stage, status: 'owner agent failed', build: null }

let verdict = null
for (let round = 0; round <= MAX_FIX_ROUNDS; round++) {
  phase('Verify')
  verdict = await agent(
    `Independently verify stage ${stage} (${scope}). Contract: ${docList}.
Owner report: ${JSON.stringify(build)}.
For every row of the "Testy" tables at levels ${levels.join('/')}, make sure a pytest test exists (write it if missing) and run the suites (run-tests skill).
Do not modify component code. Return exact counts, each failure with evidence, and the list of design claims still without a test.`,
    { agentType: 'qa-engineer', label: `verify:${stage}#${round}`, phase: 'Verify', schema: VERDICT },
  )
  if (!verdict) { log('qa-engineer did not return a verdict'); break }
  log(`round ${round}: ${verdict.passed ? 'PASS' : `${verdict.failures.length} failure(s)`}, ${verdict.untested_claims.length} untested claim(s)`)
  if (verdict.passed && verdict.untested_claims.length === 0) break
  if (round === MAX_FIX_ROUNDS) { log(`giving up after ${MAX_FIX_ROUNDS} fix rounds; needs a human decision`); break }
  phase('Fix')
  build = await agent(
    `Fix stage ${stage} (${scope}) based on the QA report below. Fix the component, not the tests, unless a test contradicts the design doc (then say so).
QA report: ${JSON.stringify({ failures: verdict.failures, untested_claims: verdict.untested_claims, report: verdict.report })}
Return: summary, changed files, doc updates, open items.`,
    { agentType: owner, label: `fix:${stage}#${round + 1}`, phase: 'Fix', schema: REPORT },
  ) || build
}

return { stage, owner, build, verdict }
