import React, { useState, useMemo } from "react";
import "./Card.css";
import { CircularProgressbar } from "react-circular-progressbar";
import "react-circular-progressbar/dist/styles.css";
import { motion, AnimateSharedLayout } from "framer-motion";
import { UilTimes } from "@iconscout/react-unicons";
import Chart from "react-apexcharts";

// react-simple-maps imports
import {
  ComposableMap,
  Geographies,
  Geography,
  Marker,
} from "react-simple-maps";

/*
  data is expected in the form:
  {
    trustpilot_data: [
      { stars: number, tp_location: string, sentiment: "positive" | "negative" | "neutral" },
      ...
    ]
  }
*/

const TrustpilotCard = ({ data, title, color, icon: Icon }) => {
  const [expanded, setExpanded] = useState(false);

  // Make sure we don't fail if data is missing
  const trustpilotData = data.trustpilot_data || [];

  // Example metric for the compact card: average stars
  const averageStars =
    trustpilotData.reduce((acc, entry) => acc + (entry.stars || 0), 0) /
    (trustpilotData.length || 1);

  // --- 1) Prepare data for the bar chart ---
  const barChartData = useMemo(() => {
    // We assume star ratings from 1 to 5; adjust if needed
    const allStars = [1, 2, 3, 4, 5];
    const sentiments = ["positive", "negative", "neutral"];
    const sentimentColors = {
      positive: "#00b300",
      negative: "#ff4d4d",
      neutral: "#cccc00",
    };

    // Count occurrences [sentiment][stars]
    // For each sentiment, we store counts in the order of allStars
    const series = sentiments.map((sentiment) => {
      const countsPerStar = allStars.map((star) => {
        return trustpilotData.filter(
          (d) => d.stars === star && d.sentiment === sentiment
        ).length;
      });
      return {
        name: sentiment,
        data: countsPerStar,
      };
    });

    return {
      series,
      options: {
        chart: {
          type: "bar",
          height: "auto",
        },
        xaxis: {
          categories: allStars,
          title: { text: "Star Rating" },
        },
        yaxis: {
          title: { text: "Count of Reviews" },
        },
        plotOptions: {
          bar: {
            horizontal: false,
            columnWidth: "50%",
            endingShape: "rounded",
          },
        },
        dataLabels: {
          enabled: false,
        },
        legend: {
          position: "top",
        },
        colors: sentiments.map((s) => sentimentColors[s]),
        grid: {
          show: true,
        },
      },
    };
  }, [trustpilotData]);

  // --- 2) Prepare data for the "geographical representation" ---
  // We'll use react-simple-maps. Each location gets a marker, color-coded by sentiment.
  // This example uses a minimal set of known coordinates for demonstration.
  const locationCoordinates = {
    // Example: "USA": [longitude, latitude]
    // You can expand or build dynamically from your data.
    USA: [-100, 40],
    Germany: [10, 51],
    France: [2, 46],
    UK: [-3, 55],
    // etc.
  };

  // Simple color map for sentiments
  const sentimentColorMap = {
    positive: "#00b300",
    negative: "#ff4d4d",
    neutral: "#cccc00",
  };

  const geoUrl =
    "https://raw.githubusercontent.com/deldersveld/topojson/master/world-countries.json";

  return (
    <AnimateSharedLayout>
      {expanded ? (
        <ExpandedCard
          title={title}
          color={color}
          barChartData={barChartData}
          trustpilotData={trustpilotData}
          geoUrl={geoUrl}
          locationCoordinates={locationCoordinates}
          sentimentColorMap={sentimentColorMap}
          setExpanded={() => setExpanded(false)}
        />
      ) : (
        <CompactCard
          title={title}
          color={color}
          icon={Icon}
          averageStars={averageStars}
          setExpanded={() => setExpanded(true)}
        />
      )}
    </AnimateSharedLayout>
  );
};

const CompactCard = ({ title, color, icon: Icon, averageStars, setExpanded }) => {
  return (
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
          value={averageStars}
          maxValue={5}
          text={averageStars.toFixed(1)}
        />
        <span>{title}</span>
      </div>
      <div className="detail">
        <Icon />
        <span>{averageStars.toFixed(2)}</span>
        <span>Average Stars</span>
      </div>
    </motion.div>
  );
};

const ExpandedCard = ({
  title,
  color,
  barChartData,
  trustpilotData,
  geoUrl,
  locationCoordinates,
  sentimentColorMap,
  setExpanded,
}) => {
  return (
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

      {/* --- First Chart: Bar Chart --- */}
      <div className="chartContainer">
        <Chart
          options={barChartData.options}
          series={barChartData.series}
          type="bar"
        />
      </div>

      {/* --- Second Chart: Geographical Representation --- */}
      <div className="chartContainer" style={{ marginTop: "2rem" }}>
        <ComposableMap
          projection="geoMercator"
          width={800}
          height={400}
          style={{ width: "100%", height: "auto" }}
        >
          <Geographies geography={geoUrl}>
            {({ geographies }) =>
              geographies.map((geo) => (
                <Geography
                  key={geo.rsmKey}
                  geography={geo}
                  style={{
                    default: { fill: "#D6D6DA", outline: "none" },
                    hover: { fill: "#F53", outline: "none" },
                    pressed: { fill: "#E42", outline: "none" },
                  }}
                />
              ))
            }
          </Geographies>
          {trustpilotData.map((entry, index) => {
            const coords = locationCoordinates[entry.tp_location];
            if (!coords) return null; // If we don't have coordinates, skip
            const color = sentimentColorMap[entry.sentiment] || "#999999";
            return (
              <Marker key={index} coordinates={coords}>
                <circle r={5} fill={color} />
              </Marker>
            );
          })}
        </ComposableMap>
      </div>
    </motion.div>
  );
};

export default TrustpilotCard;
