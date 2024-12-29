import React, { useState } from 'react';
import Header from './components/Header';
// import LineChartComponent from './components/LineChartComponent';
import BarChartComponent from './components/BarChartComponent';
import LineChartDiscreteComponent from './components/LineChartDiscreteComponent';
import CompanySelector from './components/CompanySelector';
import { Container, Box } from '@mui/material';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import { LocalizationProvider } from '@mui/x-date-pickers';
import TopWordsBarChartComponent from './components/TopWordsBarChartComponent';
import TopBigramsBarChartComponent from './components/TopBigramsBarChartComponent';
import TopTrigramsBarChartComponent from './components/TopTrigramsBarChartComponent';

function App() {
  const [selectedCompany, setSelectedCompany] = useState('');

  const handleCompanyChange = (companyName) => {
    setSelectedCompany(companyName);
  };

  return (
    <div>
      <Header />
      <Container>
        {/* Company Selector */}
        <Box my={4}>
          <CompanySelector onCompanyChange={handleCompanyChange} />
        </Box>

        {/* Pass the selectedCompany name as a prop to each chart component */}
        <LineChartDiscreteComponent companyName={selectedCompany} />
        <LocalizationProvider dateAdapter={AdapterDayjs}>
          <TopWordsBarChartComponent companyName={selectedCompany} />
          <TopBigramsBarChartComponent companyName={selectedCompany} />
          <TopTrigramsBarChartComponent companyName={selectedCompany} />
        </LocalizationProvider>
      </Container>
    </div>
  );
}

export default App;
