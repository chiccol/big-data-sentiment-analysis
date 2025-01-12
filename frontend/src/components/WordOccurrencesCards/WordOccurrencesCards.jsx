// WordOccurrenceCards.jsx
import React, { useState, useEffect } from "react";
import "./WordOccurrencesCards.css"; // Reuse the same styling used in Cards.jsx
import WordOccurrencesCard from "../WordOccurrencesCard/WordOccurrencesCard"; 
import {
  fetchTopWords,
  fetchTopBigrams,
  fetchTopTrigrams,
} from "../../Data/Data";

const WordOccurrenceCards = ({ companyName }) => {
  const [words, setWords] = useState([]);
  const [bigrams, setBigrams] = useState([]);
  const [trigrams, setTrigrams] = useState([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Fetch top words, bigrams, and trigrams when companyName changes
  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      setError(null);

      try {
        const topWordsData = await fetchTopWords(companyName);
        const bigramsData = await fetchTopBigrams(companyName);
        const trigramsData = await fetchTopTrigrams(companyName);

        setWords(topWordsData);
        setBigrams(bigramsData);
        setTrigrams(trigramsData);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, [companyName]);

  if (loading) {
    return <div className="cards">Loading...</div>;
  }

  if (error) {
    return <div className="cards">Error: {error}</div>;
  }

  // Configuration for each WordOccurrenceCard
  const cardConfigurations = [
    {
      title: "Top Words",
      data: words,
      color: {
        backGround: "linear-gradient(rgb(248, 212, 154) -146.42%, rgb(255 202 113) -46.42%)",
        boxShadow: "0px 10px 20px 0px #F9D59B",
        fill: "#388E3C",
        stroke: "#388E3C",
      },
      icon: () => <span>🗣️</span>, // Just an example icon
    },
    {
      title: "Top Bigrams",
      data: bigrams,
      color: {
        backGround: "linear-gradient(180deg, #FF919D 0%, #FC929D 100%)",
        boxShadow: "0px 10px 20px 0px #FDC0C7",
        fill: "#FBC02D",
        stroke: "#FBC02D",
      },
      icon: () => <span>🔗</span>,
    },
    {
      title: "Top Trigrams",
      data: trigrams,
      color: {
        backGround: "linear-gradient(180deg, #bb67ff 0%, #c484f3 100%)",
        boxShadow: "0px 10px 20px 0px #e0c6f5",
        fill: "#FB8C00",
        stroke: "#FB8C00",
      },
      icon: () => <span>🔎</span>,
    },
  ];

  return (
    <div className="cards">
      {cardConfigurations.map((config, index) => 
        // Render the card only if there's data
        config.data && config.data.length > 0 && (
          <WordOccurrencesCard
            key={index}
            title={config.title}
            data={config.data}
            color={config.color}
            icon={config.icon}
          />
        )
      )}
    </div>
  );
};

export default WordOccurrenceCards;
