import React, { useState } from 'react';
import Header from './components/Header';
import LineChartDiscreteComponent from './components/LineChartDiscreteComponent';
import CompanySelector from './components/CompanySelector';
import { Container, Box } from '@mui/material';
import { LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import TopWordsBarChartComponent from './components/TopWordsBarChartComponent';
import TopBigramsBarChartComponent from './components/TopBigramsBarChartComponent';
import TopTrigramsBarChartComponent from './components/TopTrigramsBarChartComponent';

function App() {
  const [selectedCompany, setSelectedCompany] = useState('');

  const handleCompanyChange = (companyName) => {
    setSelectedCompany(companyName);
  };

  return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <div>
        <Header />
        <Container>
          {/* Company Selector */}
          <Box my={4}>
            <CompanySelector onCompanyChange={handleCompanyChange} />
          </Box>

          {/* Pass the selectedCompany name as a prop to each chart component */}
          <Box my={4}>
            <LineChartDiscreteComponent companyName={selectedCompany} />
          </Box>
          <Box my={4}>
            <TopWordsBarChartComponent companyName={selectedCompany} />
          </Box>
          <Box my={4}>
            <TopBigramsBarChartComponent companyName={selectedCompany} />
          </Box>
          <Box my={4}>
            <TopTrigramsBarChartComponent companyName={selectedCompany} />
          </Box>
        </Container>
      </div>
    </LocalizationProvider>
  );
}

export default App;
