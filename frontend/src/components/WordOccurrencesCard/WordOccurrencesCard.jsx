import React, { useState } from "react";
import { motion, AnimateSharedLayout } from "framer-motion";
import Chart from "react-apexcharts";
import { UilTimes } from "@iconscout/react-unicons";
import "./WordOccurrencesCard.css"; // If you need the same styling

const WordOccurrenceCard = ({ data, title, color }) => {
  // This state determines whether the card is in compact or expanded view
  const [expanded, setExpanded] = useState(false);

  // 1. Find the word with the highest occurrences
  //    data = [{ word: "...", occurrences: number }, ... ]
  //    We assume data[0] exists; otherwise, you should handle empty arrays gracefully
  const { word: topWord, occurrences: topOccurrences } = data.reduce(
    (prev, current) =>
      current.occurrences > prev.occurrences ? current : prev,
    data[0] // Initialize `prev` as the first item
  );

  // 2. Prepare the bar chart data for ApexCharts
  const chartData = {
    options: {
      chart: {
        type: "bar",
        height: "auto",
      },
      fill: {
        colors: [color.fill],
      },
      stroke: {
        colors: [color.stroke],
      },
      xaxis: {
        // We'll map the words to the x-axis
        categories: data.map((item) => item.word),
      },
      yaxis: {
        title: {
          text: "Occurrences",
        },
      },
      grid: {
        show: true,
      },
    },
    series: [
      {
        name: "Occurrences",
        // We'll map the occurrences to the bar chart
        data: data.map((item) => item.occurrences),
      },
    ],
  };

  return (
    <AnimateSharedLayout>
      {expanded ? (
        <ExpandedWordCard
          title={title}
          color={color}
          chartData={chartData}
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
 * The Compact (default) view for the WordOccurrenceCard:
 * It simply displays the top word (with the highest occurrences).
 */
const CompactWordCard = ({ title, color, topWord, topOccurrences, setExpanded }) => {
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
 * The Expanded view shows a bar chart of the occurrences for each word.
 */
const ExpandedWordCard = ({ title, color, chartData, setExpanded }) => {
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
      <div className="chartContainer">
        <Chart
          options={chartData.options}
          series={chartData.series}
          type="bar"
          height="100%"
        />
      </div>
      <span>Occurrences Overview</span>
    </motion.div>
  );
};

export default WordOccurrenceCard;
