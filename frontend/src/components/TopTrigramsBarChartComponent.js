// TopTrigramsBarChartComponent.jsx

import React, { useState, useEffect } from 'react';
import PropTypes from 'prop-types';
import axios from 'axios';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  Cell
} from 'recharts';
import {
  CircularProgress,
  Box,
  Typography,
  Alert
} from '@mui/material';

// Helper function for bar color
const getColor = (count) => {
  if (count >= 100) return '#1E90FF'; // DodgerBlue
  if (count >= 50) return '#87CEFA';  // LightSkyBlue
  return '#ADD8E6';                    // LightBlue
};

// Customized Tooltip Component for trigrams
const CustomizedTooltip = ({ active, payload }) => {
  if (active && payload && payload.length) {
    const { trigram, count } = payload[0].payload;
    return (
      <Box
        sx={{
          backgroundColor: 'white',
          border: '1px solid #ccc',
          padding: '10px',
          borderRadius: '4px'
        }}
      >
        <Typography variant="subtitle1"><strong>Trigram:</strong> {trigram}</Typography>
        <Typography variant="subtitle1"><strong>Count:</strong> {count}</Typography>
      </Box>
    );
  }

  return null;
};

const TopTrigramsBarChartComponent = ({ companyName }) => {
  const [topTrigrams, setTopTrigrams] = useState([]);
  const [loadingTopTrigrams, setLoadingTopTrigrams] = useState(false);
  const [error, setError] = useState(null);

  // Fetch top 20 trigrams when companyName changes
  useEffect(() => {
    if (companyName) {
      const fetchTopTrigrams = async () => {
        setLoadingTopTrigrams(true);
        try {
          // Fetch from trigrams endpoint
          const response = await axios.get(`http://127.0.0.1:8000/top_triples/${companyName}`);
          setTopTrigrams(response.data.top_trigrams);
          setError(null);
          setLoadingTopTrigrams(false);
        } catch (err) {
          console.error(`Error fetching top trigrams for ${companyName}:`, err);
          if (err.response && err.response.status === 404) {
            setError(`No trigram data found for company '${companyName}'.`);
          } else {
            setError(`Failed to fetch top trigrams for '${companyName}'. Please try again later.`);
          }
          setTopTrigrams([]);
          setLoadingTopTrigrams(false);
        }
      };

      fetchTopTrigrams();
    } else {
      setTopTrigrams([]);
      setError('No company selected.');
    }
  }, [companyName]);

  // Conditional rendering
  if (loadingTopTrigrams) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="60vh">
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box m={4}>
        <Alert severity="error">{error}</Alert>
      </Box>
    );
  }

  if (!companyName) {
    return (
      <Box m={4}>
        <Alert severity="warning">No company selected. Please select a company to view the top trigrams.</Alert>
      </Box>
    );
  }

  if (topTrigrams.length === 0) {
    return (
      <Box m={4}>
        <Alert severity="info">No top trigrams data available for '{companyName}'.</Alert>
      </Box>
    );
  }

  return (
    <Box m={4}>
      <Typography variant="h5" gutterBottom>
        Top 20 Trigrams for {companyName.charAt(0).toUpperCase() + companyName.slice(1)}
      </Typography>

      <ResponsiveContainer width="100%" height={500}>
        <BarChart
          data={topTrigrams}
          margin={{ top: 20, right: 30, left: 20, bottom: 150 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="trigram"
            angle={-45}
            textAnchor="end"
            interval={0}
            height={150}
            tick={{ fontSize: 12 }}
          />
          <YAxis />
          <Tooltip content={<CustomizedTooltip />} />
          <Legend />
          <Bar dataKey="count" name="Count">
            {topTrigrams.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={getColor(entry.count)} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </Box>
  );
};

TopTrigramsBarChartComponent.propTypes = {
  companyName: PropTypes.string.isRequired
};

export default TopTrigramsBarChartComponent;
