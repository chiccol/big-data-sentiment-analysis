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
  MenuItem,
  OutlinedInput
} from '@mui/material';
import { format, parseISO } from 'date-fns';

const ITEM_HEIGHT = 48;
const ITEM_PADDING_TOP = 8;
// MUI styles for multiple selection
const MenuProps = {
  PaperProps: {
    style: {
      maxHeight: ITEM_HEIGHT * 4.5 + ITEM_PADDING_TOP,
      width: 250,
    },
  },
};

const LineChartComponent = (companyId) => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);

  // Full list of possible sources
  const allSources = ['reddit', 'trustpilot', 'youtube'];

  // Unique company names from the data
  const [companies, setCompanies] = useState([]);

  // State for user selection
  const [selectedCompany, setSelectedCompany] = useState('');
  // Now we store sources as an array, defaulting to none or some
  const [selectedSources, setSelectedSources] = useState(['reddit']);  

  /**
   * Fetch data from the backend endpoint
   */
  const fetchData = async () => {
    try {
      // Replace the endpoint as needed (Docker Compose, etc.)
      const response = await axios.get('http://127.0.0.1:8000/aggregated-postgres-data');
      const aggregatedData = response.data.aggregated_data;

      // Extract unique companies
      const uniqueCompanies = [...new Set(aggregatedData.map(entry => entry.company))];
      setCompanies(uniqueCompanies);

      // Format date to 'yyyy-MM-dd'
      const formattedData = aggregatedData.map(entry => ({
        ...entry,
        date: format(parseISO(entry.date), 'yyyy-MM-dd')
      }));

      setData(formattedData);
      setLoading(false);
    } catch (error) {
      console.error('Error fetching aggregated data:', error);
      setLoading(false);
    }
  };

  /**
   * Initial data fetch + periodic refresh every 30 seconds
   */
  useEffect(() => {
    fetchData();
    const interval = setInterval(() => {
      fetchData();
    }, 30000);
    return () => clearInterval(interval);
  }, []);

  /**
   * Filter data to the selected company and skip rows that don't match or are null
   */
  const filteredData = data.filter(entry => entry.company === selectedCompany);

  // Handlers for dropdown changes
  const handleCompanyChange = (event) => {
    setSelectedCompany(event.target.value);
  };

  const handleSourcesChange = (event) => {
    // MUI multi-select returns an array
    const {
      target: { value },
    } = event;

    // If the value is a string, split it into an array; otherwise keep array as is
    setSelectedSources(typeof value === 'string' ? value.split(',') : value);
  };

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

      {/* Multi-select for sources */}
      <FormControl sx={{ minWidth: 200, marginBottom: 2, marginLeft: 2 }}>
        <InputLabel id="source-select-label">Select Source(s)</InputLabel>
        <Select
          labelId="source-select-label"
          multiple
          value={selectedSources}
          onChange={handleSourcesChange}
          input={<OutlinedInput label="Select Source(s)" />}
          renderValue={(selected) => selected.map(capitalize).join(', ')}
          MenuProps={MenuProps}
        >
          {allSources.map((source) => (
            <MenuItem key={source} value={source}>
              {capitalize(source)}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      {/* Render line chart if company is selected */}
      {selectedCompany && (
        <ResponsiveContainer width="100%" height={400}>
          <LineChart
            data={filteredData}
            margin={{
              top: 5, right: 30, left: 20, bottom: 5,
            }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" tickFormatter={(date) => format(parseISO(date), 'MM-dd')} />
            <YAxis domain={[-1, 1]} />
            <Tooltip />
            <Legend />
            {/*
              Render multiple <Line> elements—one per selected source
            */}
            {selectedSources.map((source) => (
              <Line
                key={source}
                type="monotone"
                dataKey={source}
                stroke={getColor(source)}
                activeDot={{ r: 8 }}
                connectNulls
                name={capitalize(source)}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      )}
      {!selectedCompany && (
        <Typography variant="body1" color="textSecondary" mt={2}>
          Please select a company to view the chart.
        </Typography>
      )}
    </Box>
  );
};

// Helper function to assign colors for each source
const getColor = (source) => {
  const colorMap = {
    reddit: '#FF4500',
    trustpilot: '#1E90FF',
    youtube: '#FF0000',
  };
  return colorMap[source] || '#8884d8';
};

// Helper function to capitalize the first letter
const capitalize = (str) => str.charAt(0).toUpperCase() + str.slice(1);

export default LineChartComponent;
