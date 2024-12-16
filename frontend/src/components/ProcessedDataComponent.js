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

const PostgresDataComponent = () => {
  const [postgresData, setPostgresData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  const fetchPostgresData = async () => {
    try {
      const response = await axios.get(`http://127.0.0.1:8000/postgres-data`);
      setPostgresData(response.data.postgres_data); 
      setLoading(false);
    } catch (error) {
      console.error('Error fetching Postgres data:', error);
      setError(true);
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPostgresData();

    // Optional: Set up periodic refresh
    const interval = setInterval(() => {
      fetchPostgresData();
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
          Failed to load Postgres data. Please try again later.
        </Typography>
      </Box>
    );
  }

  return (
    <Box m={4}>
      <Typography variant="h5" gutterBottom>
        Postgres Data
      </Typography>
      <TableContainer component={Paper}>
        <Table aria-label="postgres data table">
          <TableHead>
            <TableRow>
              {postgresData.length > 0 &&
                Object.keys(postgresData[0]).map((key) => (
                  <TableCell key={key}>{key.charAt(0).toUpperCase() + key.slice(1)}</TableCell>
                ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {postgresData.map((row, index) => (
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

export default PostgresDataComponent;
