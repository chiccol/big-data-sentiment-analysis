import React, { useState, useMemo } from "react";
import "./Card.css";
import { CircularProgressbar } from "react-circular-progressbar";
import "react-circular-progressbar/dist/styles.css";
import { motion, AnimateSharedLayout } from "framer-motion";
import { UilTimes } from "@iconscout/react-unicons";
import Chart from "react-apexcharts";

/*
  data is expected in the form:
  {
    youtube_data: [
      {
        like_count: number,
        youtube_reply_count: number,
        sentiment: "positive" | "negative" | "neutral"
      },
      ...
    ]
  }
*/

const YoutubeCard = ({ data, title, color, icon: Icon }) => {
  const [expanded, setExpanded] = useState(false);

  // Safely get the array of YouTube data
  const youtubeData = data.youtube_data || [];

  // Calculate an example compact metric: average like_count
  // (Feel free to pick a different metric, e.g., average replies)
  const totalLikes = youtubeData.reduce((acc, cur) => acc + (cur.like_count || 0), 0);
  const averageLikeCount = youtubeData.length ? totalLikes / youtubeData.length : 0;

  // Prepare the bar chart data (grouped by sentiment)
  const barChartData = useMemo(() => {
    const sentiments = ["positive", "negative", "neutral"];

    // For each sentiment, compute average of like_count and youtube_reply_count
    const avgLikesBySentiment = sentiments.map((sent) => {
      const subset = youtubeData.filter((d) => d.sentiment === sent);
      if (!subset.length) return 0;
      const total = subset.reduce((sum, item) => sum + (item.like_count || 0), 0);
      return total / subset.length;
    });

    const avgRepliesBySentiment = sentiments.map((sent) => {
      const subset = youtubeData.filter((d) => d.sentiment === sent);
      if (!subset.length) return 0;
      const total = subset.reduce((sum, item) => sum + (item.youtube_reply_count || 0), 0);
      return total / subset.length;
    });

    // Two series: "Likes" and "Replies"
    return {
      series: [
        {
          name: "Likes",
          data: avgLikesBySentiment,
        },
        {
          name: "Replies",
          data: avgRepliesBySentiment,
        },
      ],
      options: {
        chart: {
          type: "bar",
          height: "auto",
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
        xaxis: {
          categories: sentiments,
          title: { text: "Sentiment" },
        },
        yaxis: {
          title: { text: "Average Count" },
          // You can adjust min/max if desired
        },
        legend: {
          position: "top",
        },
        colors: ["#008FFB", "#FF4560"], // Adjust your preferred color palette
        grid: {
          show: true,
        },
      },
    };
  }, [youtubeData]);

  return (
    <AnimateSharedLayout>
      {expanded ? (
        <ExpandedCard
          title={title}
          color={color}
          barChartData={barChartData}
          setExpanded={() => setExpanded(false)}
        />
      ) : (
        <CompactCard
          title={title}
          color={color}
          icon={Icon}
          averageLikeCount={averageLikeCount}
          setExpanded={() => setExpanded(true)}
        />
      )}
    </AnimateSharedLayout>
  );
};

const CompactCard = ({ title, color, icon: Icon, averageLikeCount, setExpanded }) => {
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
          value={averageLikeCount}
          // This maxValue is arbitrary; pick something that suits your data range
          maxValue={100}
          text={averageLikeCount.toFixed(1)}
        />
        <span>{title}</span>
      </div>
      <div className="detail">
        <Icon />
        <span>{averageLikeCount.toFixed(2)}</span>
        <span>Average Likes</span>
      </div>
    </motion.div>
  );
};

const ExpandedCard = ({ title, color, barChartData, setExpanded }) => {
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
    </motion.div>
  );
};

export default YoutubeCard;
