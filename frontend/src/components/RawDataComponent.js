import React, { useEffect, useState } from 'react';
import axios from 'axios';
import {
  CircularProgress,
  Box,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
} from '@mui/material';

const MongoDataComponent = () => {
  const [mongoData, setMongoData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  const fetchMongoData = async () => {
    try {
      const response = await axios.get(`http://127.0.0.1:8000/mongo-data`);
      setMongoData(response.data.mongo_data); // Access the "mongo_data" key from your backend response
      setLoading(false);
    } catch (error) {
      console.error('Error fetching mongo data:', error);
      setError(true);
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMongoData();

    // Optional: Set up periodic refresh
    const interval = setInterval(() => {
      fetchMongoData();
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

  if (error) {
    return (
      <Box m={4}>
        <Typography variant="h6" color="error">
          Failed to load Mongo data. Please try again later.
        </Typography>
      </Box>
    );
  }

  return (
    <Box m={4}>
      <Typography variant="h5" gutterBottom>
        Mongo Data
      </Typography>
      <TableContainer component={Paper}>
        <Table aria-label="mongo data table">
          <TableHead>
            <TableRow>
              {mongoData.length > 0 &&
                Object.keys(mongoData[0]).map((key) => (
                  <TableCell key={key}>{key.charAt(0).toUpperCase() + key.slice(1)}</TableCell>
                ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {mongoData.map((row, index) => (
              <TableRow key={index}>
                {Object.values(row).map((value, idx) => (
                  <TableCell key={idx}>{value}</TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};

export default MongoDataComponent;
