// TopWordsBarChartComponent.jsx

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

// Helper function to determine bar color based on count
const getColor = (count) => {
  if (count >= 100) return '#1E90FF'; // DodgerBlue
  if (count >= 50) return '#87CEFA';  // LightSkyBlue
  return '#ADD8E6';                    // LightBlue
};

// Customized Tooltip Component
const CustomizedTooltip = ({ active, payload, label }) => {
  if (active && payload && payload.length) {
    const { word, count } = payload[0].payload;
    return (
      <Box
        sx={{
          backgroundColor: 'white',
          border: '1px solid #ccc',
          padding: '10px',
          borderRadius: '4px'
        }}
      >
        <Typography variant="subtitle1"><strong>Word:</strong> {word}</Typography>
        <Typography variant="subtitle1"><strong>Count:</strong> {count}</Typography>
      </Box>
    );
  }

  return null;
};

const TopWordsBarChartComponent = ({ companyName }) => {
  const [topWords, setTopWords] = useState([]); // Top 20 words data
  const [loadingTopWords, setLoadingTopWords] = useState(false); // Loading state for top words
  const [error, setError] = useState(null); // Error state

  // Fetch top 20 words when companyName changes
  useEffect(() => {
    if (companyName) {
      const fetchTopWords = async () => {
        setLoadingTopWords(true);
        try {
          const response = await axios.get(`http://127.0.0.1:8000/top_words/${companyName}`);
          setTopWords(response.data.top_words);
          setError(null); // Reset error state on success
          setLoadingTopWords(false);
        } catch (err) {
          console.error(`Error fetching top words for ${companyName}:`, err);
          if (err.response && err.response.status === 404) {
            setError(`No word data found for company '${companyName}'.`);
          } else {
            setError(`Failed to fetch top words for '${companyName}'. Please try again later.`);
          }
          setTopWords([]);
          setLoadingTopWords(false);
        }
      };

      fetchTopWords();
    } else {
      setTopWords([]);
      setError('No company selected.');
    }
  }, [companyName]);

  // Conditional rendering based on loading and error states
  if (loadingTopWords) {
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
        <Alert severity="warning">No company selected. Please select a company to view the top words.</Alert>
      </Box>
    );
  }

  if (topWords.length === 0) {
    return (
      <Box m={4}>
        <Alert severity="info">No top words data available for '{companyName}'.</Alert>
      </Box>
    );
  }

  return (
    <Box m={4}>
      <Typography variant="h5" gutterBottom>
        Top 20 Words for {companyName.charAt(0).toUpperCase() + companyName.slice(1)}
      </Typography>

      <ResponsiveContainer width="100%" height={500}>
        <BarChart
          data={topWords}
          margin={{ top: 20, right: 30, left: 20, bottom: 150 }} // Increased bottom margin for labels
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="word"
            angle={-45}
            textAnchor="end"
            interval={0}
            height={150} // Increase height to accommodate rotated labels
            tick={{ fontSize: 12 }}
          />
          <YAxis />
          <Tooltip content={<CustomizedTooltip />} />
          <Legend />
          <Bar dataKey="count" name="Count">
            {topWords.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={getColor(entry.count)} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </Box>
  );
};

// Prop type validation
TopWordsBarChartComponent.propTypes = {
  companyName: PropTypes.string.isRequired
};

export default TopWordsBarChartComponent;
