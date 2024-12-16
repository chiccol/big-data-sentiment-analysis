import React, { useEffect, useState } from 'react';
import axios from 'axios';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import {
  CircularProgress,
  Box,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem
} from '@mui/material';
import { format, parseISO } from 'date-fns';

const LineChartComponent = () => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);

  // List of sources we want to allow the user to select from
  const [sources] = useState(['reddit', 'trustpilot', 'youtube']);

  // Dynamically populated list of companies from fetched data
  const [companies, setCompanies] = useState([]);

  // Track which source and company the user has selected
  const [selectedSource, setSelectedSource] = useState('reddit');
  const [selectedCompany, setSelectedCompany] = useState('');

  /**
   * Fetch data from the backend. The endpoint should return JSON in the following format:
   * {
   *   aggregated_data: [
   *     {
   *       "date": "2023-08-01T00:00:00Z",
   *       "company": "CompanyA",
   *       "reddit": 0.5,
   *       "trustpilot": -0.2,
   *       "youtube": 0.1,
   *       ...
   *     },
   *     ...
   *   ]
   * }
   */
  const fetchData = async () => {
    try {
      const response = await axios.get('http://127.0.0.1:8000/aggregated-postgres-data');
      const aggregatedData = response.data.aggregated_data;
      
      // Extract the list of unique companies for the dropdown
      const uniqueCompanies = [...new Set(aggregatedData.map(entry => entry.company))];
      setCompanies(uniqueCompanies);

      // Format the date to 'yyyy-MM-dd'
      const formattedData = aggregatedData.map(entry => ({
        ...entry,
        date: format(parseISO(entry.date), 'yyyy-MM-dd')
      }));

      setData(formattedData);
      setLoading(false);
    } catch (error) {
      console.error("Error fetching aggregated data:", error);
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();

    // Optional: periodically refresh data every 30 seconds
    const interval = setInterval(() => {
      fetchData();
    }, 30000);

    return () => clearInterval(interval);
  }, []);

  /**
   * Filter the dataset for the selected company and valid source values.
   * This ensures that the line chart only displays data for the chosen company and source.
   */
  const filteredData = data.filter(
    (entry) => entry.company === selectedCompany && entry[selectedSource] !== null
  );

  // Handlers for the dropdowns
  const handleSourceChange = (event) => {
    setSelectedSource(event.target.value);
  };

  const handleCompanyChange = (event) => {
    setSelectedCompany(event.target.value);
  };

  // If data is still loading, show a spinner
  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="60vh">
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box m={4}>
      <Typography variant="h5" gutterBottom>
        Average Sentiment Over Time
      </Typography>

      {/* Dropdown for selecting the company */}
      <FormControl sx={{ minWidth: 200, marginBottom: 2 }}>
        <InputLabel id="company-select-label">Select Company</InputLabel>
        <Select
          labelId="company-select-label"
          value={selectedCompany}
          onChange={handleCompanyChange}
          label="Select Company"
        >
          {companies.map((company) => (
            <MenuItem key={company} value={company}>
              {company}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      {/* Dropdown for selecting the source */}
      <FormControl sx={{ minWidth: 200, marginBottom: 2, marginLeft: 2 }}>
        <InputLabel id="source-select-label">Select Source</InputLabel>
        <Select
          labelId="source-select-label"
          value={selectedSource}
          onChange={handleSourceChange}
          label="Select Source"
        >
          {sources.map((source) => (
            <MenuItem key={source} value={source}>
              {capitalize(source)}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      {/* Only show the line chart if a company is selected */}
      {selectedCompany && (
        <ResponsiveContainer width="100%" height={400}>
          <LineChart
            data={filteredData}
            margin={{
              top: 5, right: 30, left: 20, bottom: 5,
            }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            {/* Format date labels to MM-dd */}
            <XAxis dataKey="date" tickFormatter={(date) => format(parseISO(date), 'MM-dd')} />
            {/* Y-axis domain from -1 to 1 for sentiment */}
            <YAxis domain={[-1, 1]} />
            <Tooltip />
            <Legend />

            <Line
              type="monotone"
              dataKey={selectedSource}
              stroke={getColor(selectedSource)}
              activeDot={{ r: 8 }}
              connectNulls
              name={capitalize(selectedSource)}
            />
          </LineChart>
        </ResponsiveContainer>
      )}
    </Box>
  );
};

// Helper function to assign colors for each source
const getColor = (source) => {
  const colorMap = {
    reddit: '#FF4500',      // OrangeRed
    trustpilot: '#1E90FF',  // DodgerBlue
    youtube: '#FF0000',     // Red
  };
  return colorMap[source] || '#8884d8';
};

// Helper function to capitalize the first letter of a string
const capitalize = (str) => str.charAt(0).toUpperCase() + str.slice(1);

export default LineChartComponent;
