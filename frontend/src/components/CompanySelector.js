import React, { useState, useEffect } from 'react';
import PropTypes from 'prop-types';
import {
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  CircularProgress,
  FormHelperText,
} from '@mui/material';

function CompanySelector({ onCompanyChange }) {
  const [companies, setCompanies] = useState([]);
  const [selectedCompany, setSelectedCompany] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    // Fetch list of companies from backend
    const fetchCompanies = async () => {
      try {
        const response = await fetch('http://localhost:8000/companies');
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        setCompanies(data.companies); // Set to array of company names
      } catch (err) {
        console.error('Error fetching companies:', err);
        setError('Failed to load companies.');
      } finally {
        setLoading(false);
      }
    };

    fetchCompanies();
  }, []);

  const handleChange = (event) => {
    const companyName = event.target.value;
    setSelectedCompany(companyName);
    onCompanyChange(companyName); // Pass company name to parent
  };

  if (loading) {
    return <CircularProgress />;
  }

  if (error) {
    return <FormHelperText error>{error}</FormHelperText>;
  }

  return (
    <FormControl fullWidth variant="outlined">
      <InputLabel id="company-select-label">Choose a Company</InputLabel>
      <Select
        labelId="company-select-label"
        id="company-select"
        value={selectedCompany}
        onChange={handleChange}
        label="Choose a Company"
      >
        <MenuItem value="">
          <em>-- Select --</em>
        </MenuItem>
        {companies.map((company) => (
          <MenuItem key={company} value={company}>
            {company}
          </MenuItem>
        ))}
      </Select>
      <FormHelperText>Select a company to view its data.</FormHelperText>
    </FormControl>
  );
}

CompanySelector.propTypes = {
  onCompanyChange: PropTypes.func.isRequired,
};

export default CompanySelector;
