import React, { useState, useEffect } from 'react';
import Header from './components/Header';
import LineChartComponent from './components/LineChartComponent';
import MongoDataComponent from './components/RawDataComponent';
import { Container } from '@mui/material';
import PostgresDataComponent from './components/ProcessedDataComponent';
import WordCloudContainer from './components/WordCloudContainer';
import BarChartComponent from './components/BarChartComponent';

function App() {

    return (
      <div>
        <Header />
        <Container>
          <LineChartComponent />
          {/* <MongoDataComponent /> */}
          <PostgresDataComponent />
          <WordCloudContainer />
          <BarChartComponent />
        </Container>
      </div>
    );
}


export default App;
