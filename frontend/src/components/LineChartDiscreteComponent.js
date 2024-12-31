// src/components/LineChartDiscreteComponent.js

import React, { useEffect, useState, useMemo } from 'react';
import axios from 'axios';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';
import {
  CircularProgress,
  Box,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  OutlinedInput,
  Grid,
  TextField // Corrected: Import TextField
} from '@mui/material';
import dayjs from 'dayjs';
import isBetween from 'dayjs/plugin/isBetween';

// MUI x-date-pickers
import { DatePicker } from '@mui/x-date-pickers/DatePicker';

// Extend dayjs with the isBetween plugin
dayjs.extend(isBetween);

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
  const [selectedSources, setSelectedSources] = useState(['reddit', 'trustpilot', 'youtube']);

  // For date range filtering
  const [startDate, setStartDate] = useState(null); // dayjs object
  const [endDate, setEndDate] = useState(null);     // dayjs object

  const allSources = ['reddit', 'trustpilot', 'youtube'];

  // Fetch aggregated data for the selected company
  useEffect(() => {
    // Define fetchData inside useEffect to avoid dependency issues
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await axios.get(`http://127.0.0.1:8000/aggregated-postgres-data/${companyName}`);
        const aggregatedData = response.data.aggregated_data;

        // Convert date strings to 'YYYY-MM-DD' format
        const formattedData = aggregatedData.map((item) => ({
          ...item,
          date: dayjs(item.date).format('YYYY-MM-DD'), // Keep as string for Recharts
        }));

        setData(formattedData);
        setLoading(false);
      } catch (error) {
        console.error('Error fetching daily aggregated data:', error);
        setLoading(false);
      }
    };

    if (companyName) {
      fetchData();
    } else {
      // If no company is selected, reset data
      setData([]);
      setLoading(false);
    }
  }, [companyName]); // Only re-run when companyName changes

  // After data is fetched, determine the min and max possible dates
  useEffect(() => {
    if (data.length > 0) {
      // Convert date strings to dayjs objects
      const dates = data.map(item => dayjs(item.date, 'YYYY-MM-DD'));

      // Find the earliest date
      const minDate = dates.reduce((a, b) => (a.isBefore(b) ? a : b));

      // Find the latest date
      const maxDate = dates.reduce((a, b) => (a.isAfter(b) ? a : b));

      setStartDate(minDate);
      setEndDate(maxDate);
    } else {
      // If data is empty, reset date filters
      setStartDate(null);
      setEndDate(null);
    }
  }, [data]);

  const handleSourcesChange = (event) => {
    const { value } = event.target;
    setSelectedSources(typeof value === 'string' ? value.split(',') : value);
  };

  // Filter data based on the selected date range
  const filteredData = useMemo(() => {
    if (!startDate || !endDate) return data;
    return data.filter((item) => {
      const itemDate = dayjs(item.date, 'YYYY-MM-DD');
      return itemDate.isBetween(startDate, endDate, 'day', '[]'); // Inclusive
    });
  }, [data, startDate, endDate]);

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
      <FormControl sx={{ minWidth: 200, marginBottom: 2, marginRight: 2 }}>
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

      {/* Date range pickers */}
      <Grid container spacing={2} sx={{ marginBottom: 2 }}>
        <Grid item xs={12} sm={6} md={3}>
          <DatePicker
            label="Start Date"
            value={startDate}
            onChange={(newValue) => {
              if (newValue && newValue.isValid()) setStartDate(newValue);
            }}
            maxDate={endDate || undefined} // Prevent picking a start date after end date
            renderInput={(params) => <TextField {...params} fullWidth />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <DatePicker
            label="End Date"
            value={endDate}
            onChange={(newValue) => {
              if (newValue && newValue.isValid()) setEndDate(newValue);
            }}
            minDate={startDate || undefined} // Prevent picking an end date before start date
            renderInput={(params) => <TextField {...params} fullWidth />}
          />
        </Grid>
      </Grid>

      <ResponsiveContainer width="100%" height={400}>
        <LineChart
          data={filteredData}
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="date"
            tickFormatter={(dateStr) => dayjs(dateStr, 'YYYY-MM-DD').format('MM-DD')}
          />
          <YAxis domain={[-1, 1]} />
          <Tooltip labelFormatter={(label) => dayjs(label, 'YYYY-MM-DD').format('MMMM DD, YYYY')} />
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
    reddit: '#FF4500',      // OrangeRed
    trustpilot: '#1E90FF',  // DodgerBlue
    youtube: '#FF0000',     // Red
  };
  return colorMap[source] || '#8884d8'; // Default color
};

// Helper function to capitalize source labels
const capitalize = (str) => str.charAt(0).toUpperCase() + str.slice(1);

export default LineChartDiscreteComponent;
