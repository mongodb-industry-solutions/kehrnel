import React from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import CodeBlock from '@theme/CodeBlock';
import useBaseUrl from '@docusaurus/useBaseUrl';

const startRuntimeCommand = `./startKehrnel`;

const runDemoCommand = `export RUNTIME_URL="\${RUNTIME_URL:-http://localhost:8080}"
export ENV_ID=dev
export DOMAIN=openehr
export STRATEGY_ID=openehr.rps_dual

# Choose one runtime binding path:
export MONGODB_URI="mongodb+srv://..."
export MONGODB_DB="openEHR_demo"
# or:
# export BINDINGS_REF="hdl:env:dev:mongo:openEHR_demo"

examples/cli/full_workflow_console.sh`;

export default function DemoPage() {
  return (
    <Layout title="CLI Demo" description="Terminal-first demo entrypoint for kehrnel">
      <main className="kehrnel-home kehrnel-demo-page">
        <section className="kehrnel-hero">
          <p className="kehrnel-demo-kicker">CLI Demo</p>
          <h1 className="kehrnel-demo-title">Run the openEHR RPS Dual demo from the terminal</h1>
          <p className="kehrnel-hero-subtitle">
            This demo is intentionally CLI-first. The page gives you the context and the exact commands to run;
            the terminal does the actual work.
          </p>
          <div className="kehrnel-hero-actions">
            <Link className="button button--primary button--lg" to={useBaseUrl('/docs/strategies/openehr/rps-dual/cli-workflows')}>
              Open Guided Workflow
            </Link>
            <Link className="button button--outline button--primary button--lg" to={useBaseUrl('/docs/getting-started/full-workflow-test')}>
              Open Full Workflow Test
            </Link>
          </div>
        </section>

        <section className="kehrnel-demo-section">
          <div className="apiCtaGrid apiCtaGrid--2">
            <article className="apiCtaCard">
              <div className="apiCtaKicker">Context</div>
              <div className="apiCtaTitle">What this page is for</div>
              <div className="apiCtaBody">
                Use this page as the entrypoint when you want a practical demo of Kehrnel without switching to a
                point-and-click UI. It gets you to the first successful run quickly, then hands off to the deeper
                walkthrough pages already in the docs.
              </div>
            </article>

            <article className="apiCtaCard">
              <div className="apiCtaKicker">Outcome</div>
              <div className="apiCtaTitle">What you will demonstrate</div>
              <div className="apiCtaBody">
                The CLI flow starts the runtime, activates <code>openehr.rps_dual</code>, exercises template and
                validation workflows, runs a strategy operation, and compiles and executes representative AQL.
              </div>
            </article>
          </div>
        </section>

        <section className="kehrnel-demo-section">
          <h2>How to run it</h2>
          <div className="kehrnel-demo-step">
            <h3>1. Open a terminal in the repository root</h3>
            <p>
              If your portal provides an embedded terminal, use that. Otherwise open your normal terminal and make
              sure you are in the <code>kehrnel</code> repository root before continuing.
            </p>
          </div>

          <div className="kehrnel-demo-step">
            <h3>2. Start the runtime</h3>
            <p>
              Run this in the first terminal and leave it running:
            </p>
            <CodeBlock language="bash">{startRuntimeCommand}</CodeBlock>
          </div>

          <div className="kehrnel-demo-step">
            <h3>3. Open a second terminal and run the demo workflow</h3>
            <p>
              Set the standard demo variables, provide either <code>MONGODB_URI</code> or <code>BINDINGS_REF</code>,
              and run the full CLI smoke flow:
            </p>
            <CodeBlock language="bash">{runDemoCommand}</CodeBlock>
          </div>
        </section>

        <section className="kehrnel-demo-section">
          <h2>What to do next</h2>
          <div className="apiCtaGrid">
            <article className="apiCtaCard">
              <div className="apiCtaKicker">Deep Dive</div>
              <div className="apiCtaTitle">CLI Workflows</div>
              <div className="apiCtaBody">
                Walk through the end-to-end RPS Dual flow step by step, with the rationale behind each command.
              </div>
              <div className="apiCtaActions">
                <Link className="button button--primary" to={useBaseUrl('/docs/strategies/openehr/rps-dual/cli-workflows')}>
                  Open CLI Workflows
                </Link>
              </div>
            </article>

            <article className="apiCtaCard">
              <div className="apiCtaKicker">Reference</div>
              <div className="apiCtaTitle">Full Workflow Test</div>
              <div className="apiCtaBody">
                Use the smoke-test page when you want the exact environment variables, expected artifacts, and test
                scope in one place.
              </div>
              <div className="apiCtaActions">
                <Link className="button button--primary" to={useBaseUrl('/docs/getting-started/full-workflow-test')}>
                  Open Full Workflow Test
                </Link>
              </div>
            </article>

            <article className="apiCtaCard">
              <div className="apiCtaKicker">Baseline</div>
              <div className="apiCtaTitle">Quick Start</div>
              <div className="apiCtaBody">
                Use the quick start when you want a shorter setup path before going deeper into the strategy-specific
                walkthrough.
              </div>
              <div className="apiCtaActions">
                <Link className="button button--primary" to={useBaseUrl('/docs/getting-started/quickstart')}>
                  Open Quick Start
                </Link>
              </div>
            </article>
          </div>
        </section>
      </main>
    </Layout>
  );
}
