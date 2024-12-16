import React, { useState, useEffect } from 'react';
import axios from 'axios';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts';
import { CircularProgress, Box, Typography } from '@mui/material';

const BarChartComponent = () => {
  const [wordData, setWordData] = useState([]);
  const [loading, setLoading] = useState(true);

  const fetchWordData = async () => {
    try {
      const response = await axios.get('http://127.0.0.1:8000/word-cloud-data');
      const data = response.data;

      // Transform the data into an array of objects
      const wordArray = Object.keys(data).map(word => ({
        word: word,
        count: data[word],
      }));

      // Optionally sort the data if not already sorted
      wordArray.sort((a, b) => b.count - a.count);

      setWordData(wordArray);
      setLoading(false);
    } catch (error) {
      console.error('Error fetching word data:', error);
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchWordData();

    // Optional: Refresh data periodically
    const interval = setInterval(() => {
      fetchWordData();
    }, 300000); // 5 minutes

    return () => clearInterval(interval);
  }, []);

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
        Most Used Words
      </Typography>
      <ResponsiveContainer width="100%" height={400}>
        <BarChart
          data={wordData}
          margin={{
            top: 5, right: 30, left: 20, bottom: 80,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="word" angle={-45} textAnchor="end" interval={0} />
          <YAxis />
          <Tooltip />
          <Legend />
          <Bar dataKey="count" fill="#8884d8" />
        </BarChart>
      </ResponsiveContainer>
    </Box>
  );
};

export default BarChartComponent;
