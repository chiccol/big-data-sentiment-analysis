import React, { useState, useEffect } from 'react';
import axios from 'axios';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts';
import {
  CircularProgress,
  Box,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField
} from '@mui/material';
import { DesktopDatePicker } from '@mui/x-date-pickers/DesktopDatePicker';
import dayjs from 'dayjs';

const BarChartComponent = () => {
  // All data from the server (unfiltered)
  const [allData, setAllData] = useState([]);
  // Loading state for the initial fetch
  const [loading, setLoading] = useState(true);

  // Distinct list of companies (derived from allData)
  const [companies, setCompanies] = useState([]);
  // Which company is selected
  const [selectedCompany, setSelectedCompany] = useState('');

  // Date filters
  const [startDate, setStartDate] = useState(null);
  const [endDate, setEndDate] = useState(null);

  // 1. Fetch all data on component mount
  useEffect(() => {
    const fetchAllData = async () => {
      try {
        const response = await axios.get('http://127.0.0.1:8000/word-cloud-data');
        // response.data => { data: [...] }
        const rawData = response.data.data; // big array
        setAllData(rawData);

        // Build a set of unique companies
        const uniqueCompanies = Array.from(new Set(rawData.map((item) => item.company)));
        setCompanies(uniqueCompanies);

        // Optionally select the first company automatically
        if (uniqueCompanies.length > 0) {
          setSelectedCompany(uniqueCompanies[0]);
        }

        setLoading(false);
      } catch (error) {
        console.error('Error fetching all data:', error);
        setLoading(false);
      }
    };

    fetchAllData();
  }, []);

  // 2. Filter data client-side whenever selectedCompany, startDate, or endDate changes
  const getFilteredData = () => {
    // We have a big array of objects: { company, word, count, date }
    return allData.filter((item) => {
      // Filter by company if selectedCompany is not empty
      if (selectedCompany && item.company !== selectedCompany) {
        return false;
      }
      // Filter by date range if both are set
      if (startDate && item.date) {
        // item.date is a string; parse it
        const itemDate = dayjs(item.date);
        if (itemDate.isBefore(dayjs(startDate), 'day')) {
          return false;
        }
      }
      if (endDate && item.date) {
        const itemDate = dayjs(item.date);
        if (itemDate.isAfter(dayjs(endDate), 'day')) {
          return false;
        }
      }

      return true;
    });
  };

  // Create a chart-friendly format from the filtered data
  // We might need to sum counts for each word, if there are duplicates
  const transformDataForChart = () => {
    const filteredData = getFilteredData();
    // Suppose we want to group by "word" (and sum "count" if multiple docs have same word)
    const wordMap = new Map();

    filteredData.forEach((item) => {
      const existing = wordMap.get(item.word) || 0;
      wordMap.set(item.word, existing + item.count);
    });

    // Convert map to array
    const wordArray = [];
    for (const [word, count] of wordMap.entries()) {
      wordArray.push({ word, count });
    }

    // Sort by count desc
    wordArray.sort((a, b) => b.count - a.count);

    return wordArray;
  };

  // Now get the final data for the chart
  const chartData = transformDataForChart();

  // 3. Handle loading
  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="60vh">
        <CircularProgress />
      </Box>
    );
  }

  // 4. If there's no data at all
  if (allData.length === 0) {
    return (
      <Box m={4}>
        <Typography variant="h5" gutterBottom>
          No data found
        </Typography>
      </Box>
    );
  }

  return (
    <Box m={4}>
      <Typography variant="h5" gutterBottom>
        Most Used Words (All Data, Client-Side Filtering)
      </Typography>

      {/** Company Selection */}
      <FormControl variant="outlined" sx={{ minWidth: 200, mb: 3, mr: 2 }}>
        <InputLabel id="company-select-label">Company</InputLabel>
        <Select
          labelId="company-select-label"
          value={selectedCompany}
          onChange={(e) => setSelectedCompany(e.target.value)}
          label="Company"
        >
          {companies.map((company) => (
            <MenuItem key={company} value={company}>
              {company}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      {/** Start Date Picker */}
      <DesktopDatePicker
        label="Start Date"
        inputFormat="YYYY-MM-DD"
        value={startDate}
        onChange={(newValue) => setStartDate(newValue)}
        renderInput={(params) => <TextField {...params} sx={{ mr: 2 }} />}
      />

      {/** End Date Picker */}
      <DesktopDatePicker
        label="End Date"
        inputFormat="YYYY-MM-DD"
        value={endDate}
        onChange={(newValue) => setEndDate(newValue)}
        renderInput={(params) => <TextField {...params} />}
      />

      {/** Chart */}
      <Box mt={4}>
        <ResponsiveContainer width="100%" height={400}>
          <BarChart
            data={chartData}
            margin={{ top: 5, right: 30, left: 20, bottom: 80 }}
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
    </Box>
  );
};

export default BarChartComponent;
