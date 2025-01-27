import React, { useState, useMemo } from "react";
import "./Card.css";
import { CircularProgressbar } from "react-circular-progressbar";
import "react-circular-progressbar/dist/styles.css";
import { motion, AnimateSharedLayout } from "framer-motion";
import { UilTimes } from "@iconscout/react-unicons";
import Chart from "react-apexcharts";

/*
  data is expected to be in the form:
  {
    reddit_data: [
      { score: number, replies: number, sentiment: string, subreddit: string },
      ...
    ]
  }
*/

const RedditCard = ({ data, title, color, icon: Icon }) => {
  const [expanded, setExpanded] = useState(false);

  const redditData = data.reddit_data || [];
  // Example metric for the compact card: average of scores
  const averageScore =
    redditData.reduce((acc, entry) => acc + (entry.score || 0), 0) /
    (redditData.length || 1);

  // Prepare data for the bar chart: count each sentiment x subreddit
  const barChartData = useMemo(() => {
    // 1) Identify unique subreddits
    const subreddits = [...new Set(redditData.map((d) => d.subreddit))];

    // 2) For each sentiment, build an array of counts matching each subreddit
    const sentiments = ["positive", "negative", "neutral"];

    // Prepare structure for series
    const series = sentiments.map((sentiment) => {
      const dataCounts = subreddits.map((sub) => {
        return redditData.filter(
          (d) => d.subreddit === sub && d.sentiment === sentiment
        ).length;
      });
      return { name: sentiment, data: dataCounts };
    });

    return {
      series,
      options: {
        chart: {
          type: "bar",
          height: "auto",
        },
        xaxis: {
          categories: subreddits,
          title: { text: "Subreddit" },
        },
        yaxis: {
          title: { text: "Count of Posts" },
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
        colors: ["#00b300", "#ff4d4d", "#cccc00"], // example custom colors
        grid: {
          show: true,
        },
      },
    };
  }, [redditData]);

  // Prepare data for the scatter plot: (score, replies), color-coded by sentiment
  const scatterChartData = useMemo(() => {
    const sentiments = ["positive", "negative", "neutral"];
    const colorsMap = {
      positive: "#00b300",
      negative: "#ff4d4d",
      neutral: "#cccc00",
    };

    // Build a separate series for each sentiment
    const series = sentiments.map((sentiment) => {
      const points = redditData
        .filter((d) => d.sentiment === sentiment)
        .map((d) => {
          return { x: d.score || 0, y: d.replies || 0 };
        });
      return {
        name: sentiment,
        data: points,
      };
    });

    return {
      series,
      options: {
        chart: {
          type: "scatter",
          height: "auto",
          zoom: {
            enabled: true,
            type: "xy",
          },
        },
        xaxis: {
          title: { text: "Score" },
        },
        yaxis: {
          title: { text: "Replies" },
        },
        legend: {
          position: "top",
        },
        colors: sentiments.map((s) => colorsMap[s]),
        dataLabels: {
          enabled: false,
        },
        grid: {
          show: true,
        },
      },
    };
  }, [redditData]);

  return (
    <AnimateSharedLayout>
      {expanded ? (
        <ExpandedCard
          title={title}
          color={color}
          barChartData={barChartData}
          scatterChartData={scatterChartData}
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

const CompactCard = ({ title, color, icon: Icon, averageScore, setExpanded }) => {
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
          value={averageScore}
          // Just an example formatting; adapt as you like
          text={`${averageScore.toFixed(1)}`}
          maxValue={100}
          minValue={0}
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
};

const ExpandedCard = ({ title, color, barChartData, scatterChartData, setExpanded }) => {
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

      {/* Bar Chart */}
      <div className="chartContainer">
        <Chart
          options={barChartData.options}
          series={barChartData.series}
          type="bar"
        />
      </div>

      {/* Scatter Chart */}
      <div className="chartContainer">
        <Chart
          options={scatterChartData.options}
          series={scatterChartData.series}
          type="scatter"
        />
      </div>
    </motion.div>
  );
};

export default RedditCard;
