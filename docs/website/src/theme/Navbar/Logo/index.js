import React from 'react';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';

function KehrnelLogo() {
  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      fontSize: '1.4rem',
      fontWeight: 'bold',
      fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
      letterSpacing: '-0.5px',
    }}>
      <span style={{ color: '#4A9EBD' }}>{`{ `}</span>
      <span style={{ color: '#EA6635' }}>k</span>
      <span style={{ color: '#4A9EBD' }}>e</span>
      <span style={{ color: '#4A9EBD' }}>h</span>
      <span style={{ color: '#4A9EBD' }}>r</span>
      <span style={{ color: '#EA6635' }}>n</span>
      <span style={{ color: '#EA6635' }}>e</span>
      <span style={{ color: '#EA6635' }}>l</span>
      <span style={{ color: '#4A9EBD' }}>{` }`}</span>
    </div>
  );
}

export default function Logo() {
  const baseUrl = useBaseUrl('/');

  return (
    <Link
      to={baseUrl}
      style={{
        display: 'flex',
        alignItems: 'center',
        textDecoration: 'none',
      }}
    >
      <KehrnelLogo />
    </Link>
  );
}
