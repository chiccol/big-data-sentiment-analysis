import React, { useState } from "react";
import "./Table.css";
import Paper from "@mui/material/Paper";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";

/**
 * Expects a prop: companyName (string)
 * Example structure from the server:
 * {
 *   summary: {
 *     positive: {
 *       reddit: {
 *         topicA: "summary text",
 *         topicB: "summary text",
 *         ...
 *       },
 *       trustpilot: { ... },
 *       youtube: { ... }
 *     },
 *     negative: { ... },
 *     neutral: { ... }
 *   }
 * }
 */

export default function SummaryTable({ companyName }) {
  const [summaryData, setSummaryData] = useState(null);
  const [error, setError] = useState("");

  // Handler for the "Generate Summary" button
  const handleGenerateSummary = async () => {
    setError("");
    setSummaryData(null);

    try {
      const res = await fetch(`http://localhost:8000/ask_summary/${companyName}`);
      if (!res.ok) {
        throw new Error(`Request failed with status ${res.status}`);
      }
      const data = await res.json();
      setSummaryData(data.summary || {});
    } catch (err) {
      setError(err.message);
    }
  };

  // Handler for the "Read Summary" button
  const handleReadSummary = async () => {
    setError("");
    setSummaryData(null);

    try {
      const res = await fetch(`http://localhost:8000/read_summary/${companyName}`);
      if (!res.ok) {
        throw new Error(`Request failed with status ${res.status}`);
      }
      const data = await res.json();
      setSummaryData(data.summary || {});
    } catch (err) {
      setError(err.message);
    }
  };

  // Helper function to render rows based on the nested structure
  const renderSummaryRows = () => {
    if (!summaryData) return null;

    const rows = [];
    Object.entries(summaryData).forEach(([sentiment, sources]) => {
      Object.entries(sources).forEach(([source, topics]) => {
        Object.entries(topics).forEach(([topic, text]) => {
          rows.push(
            <TableRow key={`${sentiment}-${source}-${topic}`}>
              <TableCell className="custom-table-cell">{sentiment}</TableCell>
              <TableCell className="custom-table-cell">{source}</TableCell>
              <TableCell className="custom-table-cell">{topic}</TableCell>
              <TableCell className="custom-table-cell">{text}</TableCell>
            </TableRow>
          );
        });
      });
    });
    return rows;
  };

  return (
    <div className="summary-wrapper">
      <div className="summary-header">
        <Typography variant="h5" className="summary-title">
          Summary Dashboard
        </Typography>
        <Typography variant="subtitle1" className="summary-subtitle">
          Showing summary data for: <strong>{companyName}</strong>
        </Typography>
      </div>

      <div className="summary-controls">
        <Button
          variant="contained"
          onClick={handleGenerateSummary}
          style={{ backgroundColor: "#42A5F5", marginRight: "1rem" }}
        >
          Generate Summary
        </Button>
        <Button
          variant="contained"
          onClick={handleReadSummary}
          style={{ backgroundColor: "#AB47BC" }}
        >
          Read Summary
        </Button>
      </div>

      {error && <div className="error-message">Error: {error}</div>}

      {summaryData && (
        <TableContainer
          component={Paper}
          className="table-container"
          style={{ marginTop: "1rem" }}
        >
          <Table aria-label="summary table">
            <TableHead>
              <TableRow>
                <TableCell className="custom-header-cell">Sentiment</TableCell>
                <TableCell className="custom-header-cell">Source</TableCell>
                <TableCell className="custom-header-cell">Topic</TableCell>
                <TableCell className="custom-header-cell">Summary</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>{renderSummaryRows()}</TableBody>
          </Table>
        </TableContainer>
      )}
    </div>
  );
}
