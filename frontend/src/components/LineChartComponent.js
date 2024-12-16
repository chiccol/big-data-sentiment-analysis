import React, { useEffect, useState } from 'react';
import axios from 'axios';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import { CircularProgress, Box, Typography, FormControl, InputLabel, Select, MenuItem } from '@mui/material';
import { format, parseISO } from 'date-fns';

const LineChartComponent = () => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [sources] = useState(['reddit', 'trustpilot', 'youtube']);
  const [selectedSource, setSelectedSource] = useState('reddit');

  const fetchData = async () => {
    try {
      const response = await axios.get('http://127.0.0.1:8000/aggregated-postgres-data');
      const aggregatedData = response.data.aggregated_data;

      // Optional: Format dates if needed
      const formattedData = aggregatedData.map(entry => ({
        ...entry,
        date: format(parseISO(entry.date), 'yyyy-MM-dd') // Ensure consistent date format
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

    // Set up periodic refresh every 5 minutes
    const interval = setInterval(() => {
      fetchData();
    }, 300000); // 300,000 ms = 5 minutes

    return () => clearInterval(interval);
  }, []);

  const handleSourceChange = (event) => {
    setSelectedSource(event.target.value);
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
      <FormControl sx={{ minWidth: 200, marginBottom: 2 }}>
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
      <ResponsiveContainer width="100%" height={400}>
        <LineChart
          data={data}
          margin={{
            top: 5, right: 30, left: 20, bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" tickFormatter={(date) => format(parseISO(date), 'MM-dd')} />
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
    </Box>
  );
};

// Helper function to assign colors based on source
const getColor = (source) => {
  const colorMap = {
    reddit: '#FF4500',      // OrangeRed
    trustpilot: '#1E90FF',  // DodgerBlue
    youtube: '#FF0000',     // Red
  };
  return colorMap[source] || '#8884d8';
};

// Helper function to capitalize the first letter
const capitalize = (str) => str.charAt(0).toUpperCase() + str.slice(1);

export default LineChartComponent;
