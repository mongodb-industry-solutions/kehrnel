import React from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';

export default function Home() {
  return (
    <Layout title="{kehrnel}" description="Multi-strategy healthcare data runtime">
      <main className="kehrnel-home">
        <section className="kehrnel-hero">
          <img src={useBaseUrl('/img/logo.svg')} alt="{kehrnel} Logo" className="kehrnel-hero-logo" />
          <p className="kehrnel-hero-claim">
            Multi-strategy, multi-domain healthcare data runtime.
          </p>
          <p className="kehrnel-hero-subtitle">
            Build, activate, and operate persistence strategies for structured and unstructured healthcare data workflows.
          </p>
          <div className="kehrnel-hero-actions">
            <Link className="button button--primary button--lg" to={useBaseUrl('/docs')}>
              Documentation
            </Link>
            <Link className="button button--secondary button--lg" to={useBaseUrl('/docs/api/endpoint-catalog')}>
              API Catalog
            </Link>
            <a className="button button--secondary button--lg" href="/docs" target="_blank" rel="noopener noreferrer">
              Swagger UI
            </a>
            <a className="button button--secondary button--lg" href="/redoc" target="_blank" rel="noopener noreferrer">
              ReDoc
            </a>
          </div>
        </section>

        <section className="kehrnel-grid">
          <article className="kehrnel-card">
            <h2>Strategy Packs</h2>
            <p>Persistence behavior is defined by pack contracts (`manifest.json`, `spec.json`, schema/defaults).</p>
            <Link to={useBaseUrl('/docs/strategies/overview')}>Explore strategies</Link>
          </article>
          <article className="kehrnel-card">
            <h2>Data Sampling</h2>
            <p>Sampling and synthetic job APIs support fast validation, dry runs, and quality checks.</p>
            <Link to={useBaseUrl('/docs/concepts/data-sampling')}>Sampling model</Link>
          </article>
          <article className="kehrnel-card">
            <h2>Platform Vision</h2>
            <p>From standards-based CDRs to hybrid rules + LLM extraction and AI-ready operations.</p>
            <Link to={useBaseUrl('/docs/vision/roadmap')}>Read roadmap</Link>
          </article>
          <article className="kehrnel-card">
            <h2>Licensing</h2>
            <p>Runtime code under Apache 2.0; strategy knowledge artifacts can be Creative Commons.</p>
            <Link to={useBaseUrl('/docs/vision/licensing')}>Licensing model</Link>
          </article>
        </section>
      </main>
    </Layout>
  );
}
