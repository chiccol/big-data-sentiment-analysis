import React, { useState } from "react";
import { motion, AnimateSharedLayout } from "framer-motion";
import { UilTimes } from "@iconscout/react-unicons";
import Wordcloud from "react-wordcloud";
import "./WordOccurrencesCard.css"; // Same styling, if needed

const WordOccurrenceCard = ({ data, title, color }) => {
  // This state determines whether the card is in compact or expanded view
  const [expanded, setExpanded] = useState(false);

  // 1. Find the word with the highest occurrences
  const { word: topWord, occurrences: topOccurrences } = data.reduce(
    (prev, current) =>
      current.occurrences > prev.occurrences ? current : prev,
    data[0]
  );

  // 2. Prepare the data in a shape suitable for react-wordcloud
  //    The library expects an array of objects: [{ text: "word", value: someNumber }, ...]
  const wordCloudData = data.map(({ word, occurrences }) => ({
    text: word,
    value: occurrences,
  }));

  return (
    <AnimateSharedLayout>
      {expanded ? (
        <ExpandedWordCard
          title={title}
          color={color}
          wordCloudData={wordCloudData}
          setExpanded={() => setExpanded(false)}
        />
      ) : (
        <CompactWordCard
          title={title}
          color={color}
          topWord={topWord}
          topOccurrences={topOccurrences}
          setExpanded={() => setExpanded(true)}
        />
      )}
    </AnimateSharedLayout>
  );
};

/** 
 * Compact (default) view
 */
const CompactWordCard = ({
  title,
  color,
  topWord,
  topOccurrences,
  setExpanded,
}) => {
  return (
    <motion.div
      className="CompactCard"
      style={{
        background: color.backGround,
        boxShadow: color.boxShadow,
      }}
      layoutId="expandableWordCard"
      onClick={setExpanded}
    >
      <div className="detail">
        <span style={{ fontWeight: "bold", fontSize: "1.2rem" }}>{title}</span>
        <span>Top Word: {topWord}</span>
        <span>Occurrences: {topOccurrences}</span>
      </div>
    </motion.div>
  );
};

/**
 * Expanded view showing the word cloud
 */
const ExpandedWordCard = ({ title, color, wordCloudData, setExpanded }) => {
  // Wordcloud options (optional styling/behavior customizations)
  const options = {
    rotations: 2,
    rotationAngles: [-90, 0],
    fontSizes: [12, 52], // Min and max font size
  };

  return (
    <motion.div
      className="ExpandedCard"
      style={{
        background: color.backGround,
        boxShadow: color.boxShadow,
      }}
      layoutId="expandableWordCard"
    >
      <div style={{ alignSelf: "flex-end", cursor: "pointer", color: "black" }}>
        <UilTimes onClick={setExpanded} />
      </div>
      <span>{title}</span>
      <div className="chartContainer" style={{ width: "100%", height: "100%" }}>
        <Wordcloud
          options={options}
          words={wordCloudData}
        />
      </div>
      <span>Occurrences Overview</span>
    </motion.div>
  );
};

export default WordOccurrenceCard;
