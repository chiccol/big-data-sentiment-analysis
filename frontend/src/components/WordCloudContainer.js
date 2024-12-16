import React, { useState, useEffect } from 'react';
import axios from 'axios';
import WordCloud from './WordCloud';

const WordCloudContainer = () => {
  const [wordCounts, setWordCounts] = useState([]);
  const [loading, setLoading] = useState(true);

  const fetchWordCloudData = async () => {
    try {
      const response = await axios.get('http://127.0.0.1:8000/word-cloud-data');
      const data = response.data;

      // Convert data to the format expected by WordCloud
      const words = Object.keys(data).map(word => ({
        text: word,
        value: data[word],
      }));

      setWordCounts(words);
      setLoading(false);
    } catch (error) {
      console.error('Error fetching word cloud data:', error);
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchWordCloudData();
  }, []);

  if (loading) {
    return <div>Loading word cloud...</div>;
  }

  return <WordCloud words={wordCounts} />;
};

export default WordCloudContainer;
