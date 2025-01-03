import React, { useState } from 'react';
import { Container, Grid2, Box, CssBaseline } from '@mui/material';
import { LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import Header from './components/Header';
// import Sidebar from './components/Sidebar';
import ChartCard from './components/ChartCard';
import LinearChartScoresComponent from './components/LinearChartScoresComponent';
import LineChartDiscreteComponent from './components/LineChartDiscreteComponent';
import TopWordsBarChartComponent from './components/TopWordsBarChartComponent';
import TopBigramsBarChartComponent from './components/TopBigramsBarChartComponent';
import TopTrigramsBarChartComponent from './components/TopTrigramsBarChartComponent';
import CompanySelector from './components/CompanySelector';
import SummariesButton from './components/SummaryComponent';

function App() {
  const [selectedCompany, setSelectedCompany] = useState('');

  const handleCompanyChange = (companyName) => {
    setSelectedCompany(companyName);
  };

  return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <CssBaseline />
      <div style={{ display: 'flex' }}>
        {/* <Sidebar /> */}
        <Box sx={{ flexGrow: 1 }}>
          <Header />
          <Container>
            {/* Company Selector */}
            <Box my={4}>
              <CompanySelector onCompanyChange={handleCompanyChange} />
            </Box>

            {/* Line Charts */}
            <Grid2 container spacing={4}>
              <Grid2 item xs={12} >
                <ChartCard title="Line Chart Scores">
                  <LinearChartScoresComponent companyName={selectedCompany} />
                </ChartCard>
              </Grid2>
              <Grid2 item xs={12} >
                <ChartCard title="Line Chart Discrete">
                  <LineChartDiscreteComponent companyName={selectedCompany} />
                </ChartCard>
              </Grid2>
            </Grid2>

            {/* Bar Charts */}
            <Grid2 container spacing={4} mt={4}>
              <Grid2 item xs={12} >
                <ChartCard title="Top Words">
                  <TopWordsBarChartComponent companyName={selectedCompany} />
                </ChartCard>
              </Grid2>
              <Grid2 item xs={12} >
                <ChartCard title="Top Bigrams">
                  <TopBigramsBarChartComponent companyName={selectedCompany} />
                </ChartCard>
              </Grid2>
              <Grid2 item xs={12} >
                <ChartCard title="Top Trigrams">
                  <TopTrigramsBarChartComponent companyName={selectedCompany} />
                </ChartCard>
              </Grid2>
            </Grid2>
            <SummariesButton companyName={selectedCompany} />
          </Container>
        </Box>
      </div>
    </LocalizationProvider>
  );
}

export default App;
