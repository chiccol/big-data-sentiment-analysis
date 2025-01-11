import React, { useState } from "react";
import "./Card.css";
import { CircularProgressbar } from "react-circular-progressbar";
import "react-circular-progressbar/dist/styles.css";
import { motion, AnimateSharedLayout } from "framer-motion";
import { UilTimes } from "@iconscout/react-unicons";
import Chart from "react-apexcharts";

const Card = ({ data, title, color, icon: Icon }) => {
  const [expanded, setExpanded] = useState(false);

  const averageScore =
    data.reduce((acc, entry) => acc + (entry.score || 0), 0) / data.length;

  const chartData = {
    options: {
      chart: {
        type: "area",
        height: "auto",
      },
      fill: {
        colors: [color.fill],
        type: "gradient",
      },
      stroke: {
        curve: "smooth",
        colors: [color.stroke],
      },
      xaxis: {
        type: "datetime",
        categories: data.map((entry) => entry.date),
      },
      grid: {
        show: true,
      },
      yaxis: {
        min: -1,
        max: 1,
      },
    },
    series: [
      {
        name: title,
        data: data.map((entry) => entry.score || 0),
      },
    ],
  };

  return (
    <AnimateSharedLayout>
      {expanded ? (
        <ExpandedCard
          title={title}
          color={color}
          chartData={chartData}
          setExpanded={() => setExpanded(false)}
        />
      ) : (
        <CompactCard
          title={title}
          color={color}
          icon={Icon}
          averageScore={averageScore}
          setExpanded={() => setExpanded(true)}
        />
      )}
    </AnimateSharedLayout>
  );
};

const CompactCard = ({ title, color, icon: Icon, averageScore, setExpanded }) => (
  <motion.div
    className="CompactCard"
    style={{
      background: color.backGround,
      boxShadow: color.boxShadow,
    }}
    layoutId="expandableCard"
    onClick={setExpanded}
  >
    <div className="radialBar">
      <CircularProgressbar
        value={averageScore}
        text={`${averageScore.toFixed(1)}%`}
      />
      <span>{title}</span>
    </div>
    <div className="detail">
      <Icon />
      <span>{averageScore.toFixed(2)}</span>
      <span>Average Score</span>
    </div>
  </motion.div>
);

const ExpandedCard = ({ title, color, chartData, setExpanded }) => (
  <motion.div
    className="ExpandedCard"
    style={{
      background: color.backGround,
      boxShadow: color.boxShadow,
    }}
    layoutId="expandableCard"
  >
    <div style={{ alignSelf: "flex-end", cursor: "pointer", color: "black" }}>
      <UilTimes onClick={setExpanded} />
    </div>
    <span>{title}</span>
    <div className="chartContainer">
      <Chart options={chartData.options} series={chartData.series} type="area" />
    </div>
    <span>Last 24 hours</span>
  </motion.div>
);

export default Card;
