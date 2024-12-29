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
const MenuProps = {
  PaperProps: {
    style: {
      maxHeight: ITEM_HEIGHT * 4.5 + ITEM_PADDING_TOP,
      width: 250,
    },
  },
};

const LineChartDiscreteComponent = ({ companyName }) => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedSources, setSelectedSources] = useState(['reddit', 'trustpilot', 'youtube']); // Default to all sources

  const allSources = ['reddit', 'trustpilot', 'youtube']; // Available sentiment sources

  // Fetch aggregated data for the selected company
  const fetchData = async () => {
    try {
      const response = await axios.get(`http://127.0.0.1:8000/aggregated-postgres-data/${companyName}`);
      const aggregatedData = response.data.aggregated_data;

      // Format dates in the data for consistency
      const formattedData = aggregatedData.map((item) => ({
        ...item,
        date: format(parseISO(item.date), 'yyyy-MM-dd'),
      }));

      setData(formattedData);
      setLoading(false);
    } catch (error) {
      console.error('Error fetching daily aggregated data:', error);
      setLoading(false);
    }
  };

  useEffect(() => {
    if (companyName) {
      fetchData();
    }
  }, [companyName]); // Refetch data when companyName changes

  const handleSourcesChange = (event) => {
    const { value } = event.target;
    setSelectedSources(typeof value === 'string' ? value.split(',') : value);
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="60vh">
        <CircularProgress />
      </Box>
    );
  }

  if (!companyName) {
    return (
      <Box m={4}>
        <Typography variant="h6" color="error">
          No company selected. Please select a company to view the data.
        </Typography>
      </Box>
    );
  }

  if (data.length === 0) {
    return (
      <Box m={4}>
        <Typography variant="h6" color="textSecondary">
          No data available for the selected company: {companyName}.
        </Typography>
      </Box>
    );
  }

  return (
    <Box m={4}>
      <Typography variant="h5" gutterBottom>
        Daily Aggregated Sentiment Scores for {capitalize(companyName)}
      </Typography>

      {/* Multi-select for sources */}
      <FormControl sx={{ minWidth: 200, marginBottom: 2 }}>
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

      <ResponsiveContainer width="100%" height={400}>
        <LineChart
          data={data}
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="date"
            tickFormatter={(dateStr) => format(parseISO(dateStr), 'MM-dd')}
          />
          <YAxis domain={[-1, 1]} /> {/* Updated domain */}
          <Tooltip />
          <Legend />

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
    </Box>
  );
};

// Helper function for line colors
const getColor = (source) => {
  const colorMap = {
    reddit: '#FF4500',       // OrangeRed
    trustpilot: '#1E90FF',   // DodgerBlue
    youtube: '#FF0000',      // Red
  };
  return colorMap[source] || '#8884d8'; // Default color
};

// Helper function to capitalize source labels
const capitalize = (str) => str.charAt(0).toUpperCase() + str.slice(1);

export default LineChartDiscreteComponent;
