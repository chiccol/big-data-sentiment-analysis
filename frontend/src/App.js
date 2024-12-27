import React, { useState, useEffect } from 'react';
import Header from './components/Header';
import LineChartComponent from './components/LineChartComponent';
// import MongoDataComponent from './components/RawDataComponent';
import { Container } from '@mui/material';
// import PostgresDataComponent from './components/ProcessedDataComponent';
import WordCloudComponent from './components/WordCloudComponent';
import BarChartComponent from './components/BarChartComponent';
import LineChartDiscreteComponent from './components/LineChartDiscreteComponent';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import { LocalizationProvider } from '@mui/x-date-pickers';

function App() {

    return (
      <div>
        <Header />
        <Container>
          <LineChartDiscreteComponent />
          <LineChartComponent />
          {/* <MongoDataComponent /> */}
          {/* <PostgresDataComponent /> */}
          <LocalizationProvider dateAdapter={AdapterDayjs}>
            <WordCloudComponent />
          </LocalizationProvider>
          <LocalizationProvider dateAdapter={AdapterDayjs}>
          <BarChartComponent />
          </LocalizationProvider>
        </Container>
      </div>
    );
}


export default App;
