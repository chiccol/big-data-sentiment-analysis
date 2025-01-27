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
import TextField from "@mui/material/TextField";

/**
 * Expects a prop: companyName (string)
 * Example structure from the server:
 * {
 *   summary: {
 *     positive: {
 *       reddit: {
 *         'Customer service': "summary text",
 *         'Product quality': "summary text",
 *         'Price': "summary text",
 *         'General': "summary text",
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
  const [summaryData, setSummaryData] = useState({});
  const [error, setError] = useState("");

  // NEW: State variables for date range
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");

  // Handler for the "Generate Summary" button
  const handleGenerateSummary = async () => {
    setError("");
    setSummaryData(null);

    try {
      // Pass the startDate and endDate as query parameters (or in a POST body if your backend expects that).
      // For example, as GET parameters:
      const res = await fetch(
        `http://localhost:8000/trigger_summary/${companyName}?start_date=${encodeURIComponent(
          startDate
        )}&end_date=${encodeURIComponent(endDate)}`
      );

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
      // If the "read_summary" endpoint also supports date filtering, you could do something similar.
      // Otherwise, just call it as before:
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

  // Define the "dimensions" of our table data:
  const sentiments = ["positive", "negative", "neutral"];
  const sources = ["trustpilot", "reddit", "youtube"];
  const topics = ["Customer service", "Product quality", "Price", "General"];

  // Render a separate table for each sentiment
  const renderTables = () => {
    return sentiments.map((sentiment) => (
      <TableContainer
        component={Paper}
        className="table-container"
        style={{ marginTop: "1rem" }}
        key={sentiment}
      >
        <Typography variant="h6" style={{ margin: "1rem" }}>
          {sentiment.charAt(0).toUpperCase() + sentiment.slice(1)} Sentiment
        </Typography>
        <Table aria-label={`${sentiment}-table`}>
          <TableHead>
            <TableRow>
              <TableCell className="custom-header-cell">Topic</TableCell>
              {sources.map((source) => (
                <TableCell className="custom-header-cell" key={source}>
                  {source}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {topics.map((topic) => (
              <TableRow key={topic}>
                <TableCell className="custom-table-cell">{topic}</TableCell>
                {sources.map((source) => (
                  <TableCell className="custom-table-cell" key={source}>
                    {
                      summaryData[sentiment] &&
                      summaryData[sentiment][source] &&
                      summaryData[sentiment][source][topic]
                        ? summaryData[sentiment][source][topic]
                        : "No data"
                    }
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    ));
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
        <TextField
          label="Start Date"
          type="date"
          value={startDate}
          onChange={(e) => setStartDate(e.target.value)}
          InputLabelProps={{
            shrink: true,
          }}
          style={{ marginRight: "1rem" }}
        />
        <TextField
          label="End Date"
          type="date"
          value={endDate}
          onChange={(e) => setEndDate(e.target.value)}
          InputLabelProps={{
            shrink: true,
          }}
          style={{ marginRight: "1rem" }}
        />

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

      {/* Only render the tables if we have summaryData */}
      {summaryData && Object.keys(summaryData).length > 0 && (
        <div style={{ marginTop: "2rem" }}>{renderTables()}</div>
      )}

      {/* Show a "no data" message if summaryData is empty */}
      {summaryData && Object.keys(summaryData).length === 0 && (
        <TableContainer
          component={Paper}
          className="table-container"
          style={{ marginTop: "1rem" }}
        >
          <Table>
            <TableBody>
              <TableRow>
                <TableCell colSpan={4} style={{ textAlign: "center" }}>
                  No data available
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </div>
  );
}
