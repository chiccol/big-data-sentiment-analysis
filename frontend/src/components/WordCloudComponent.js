import React, { useState, useEffect } from 'react';
import axios from 'axios';
import {
  Box,
  Typography,
  CircularProgress,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField
} from '@mui/material';
import { DesktopDatePicker } from '@mui/x-date-pickers/DesktopDatePicker';
import dayjs from 'dayjs';

// Import the WordCloud component from react-d3-cloud
import WordCloud from 'react-d3-cloud';

const WordCloudComponent = () => {
  // State to hold all raw data
  const [allData, setAllData] = useState([]);
  // Loading state
  const [loading, setLoading] = useState(true);
  
  // Distinct list of companies
  const [companies, setCompanies] = useState([]);
  // Selected company
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
        const rawData = response.data.data;
        setAllData(rawData);

        // Build a set of unique companies
        const uniqueCompanies = Array.from(new Set(rawData.map(item => item.company)));
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

  // 2. Filter data client-side
  const getFilteredData = () => {
    // We have array of { company, word, count, date }
    return allData.filter(item => {
      // Filter by company if selectedCompany is not empty
      if (selectedCompany && item.company !== selectedCompany) {
        return false;
      }
  
     // If no dates are selected, skip date filtering and include all items for this company
      if (!startDate && !endDate) {
        return true;
      }
  
      // Otherwise, apply date filters (if set)
      if (startDate && item.date) {
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

  // 3. Transform to the format react-d3-cloud expects: { text, value }
  const transformDataForWordCloud = () => {
    const filtered = getFilteredData();
    // Sum counts by word if duplicates exist
    const wordMap = new Map();

    filtered.forEach(item => {
      const existing = wordMap.get(item.word) || 0;
      wordMap.set(item.word, existing + item.count);
    });

    return [...wordMap.entries()].map(([word, count]) => ({
      text: word,
      value: count
    }));
  };

  // This will be used by the WordCloud component
  const wordCloudData = transformDataForWordCloud();

  // Font sizing: you can tweak this to your liking
  const fontSizeMapper = (word) => {
    // For example, use a simple log scale
    return Math.log2(word.value + 1) * 20;
  };

  // Optionally rotate words
  const rotate = (word) => {
    return word.value % 2 === 0 ? 0 : 90;
  };

  // 4. Handle loading
  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="60vh">
        <CircularProgress />
      </Box>
    );
  }

  // 5. If there’s no data
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
        Word Cloud (Client-Side Filtering)
      </Typography>

      {/* Company Selection */}
      <FormControl variant="outlined" sx={{ minWidth: 200, mb: 3, mr: 2 }}>
        <InputLabel id="company-select-label">Company</InputLabel>
        <Select
          labelId="company-select-label"
          value={selectedCompany}
          onChange={(e) => setSelectedCompany(e.target.value)}
          label="Company"
        >
          {companies.map(company => (
            <MenuItem key={company} value={company}>
              {company}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      {/* Start Date Picker */}
      <DesktopDatePicker
        label="Start Date"
        inputFormat="YYYY-MM-DD"
        value={startDate}
        onChange={(newValue) => setStartDate(newValue)}
        renderInput={(params) => <TextField {...params} sx={{ mr: 2 }} />}
      />

      {/* End Date Picker */}
      <DesktopDatePicker
        label="End Date"
        inputFormat="YYYY-MM-DD"
        value={endDate}
        onChange={(newValue) => setEndDate(newValue)}
        renderInput={(params) => <TextField {...params} />}
      />

      {/* Word Cloud */}
      <Box mt={4} display="flex" justifyContent="center">
        <WordCloud
          data={wordCloudData}
          fontSizeMapper={fontSizeMapper}
          rotate={rotate}
          padding={2}
          width={800}
          height={600}
        />
      </Box>
    </Box>
  );
};

export default WordCloudComponent;
