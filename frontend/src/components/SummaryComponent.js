// SummariesButton.jsx
import React, { useState } from 'react';

function SummariesButton({ companyName }) {
  const [company, setCompany] = useState(companyName);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleGetSummary = async () => {
    setLoading(true);
    setError(null);
    setSummary(null);

    try {
      // Call your FastAPI endpoint
      const res = await fetch(`http://localhost:8000/summaries/${company}`);
      if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
      }
      const data = await res.json();
      setSummary(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: "1rem" }}>
      <input
        type="text"
        value={company}
        onChange={(e) => setCompany(e.target.value)}
        placeholder="Enter company name"
      />
      <button onClick={handleGetSummary}>Get Summary</button>

      {loading && <p>Loading...</p>}
      {error && <p style={{color: "red"}}>Error: {error}</p>}
      {summary && (
        <div>
          <h3>Summary for {summary.company}</h3>
          <pre>{JSON.stringify(summary, null, 2)}</pre>
        </div>
      )}
    </div>
  );
}

export default SummariesButton;
