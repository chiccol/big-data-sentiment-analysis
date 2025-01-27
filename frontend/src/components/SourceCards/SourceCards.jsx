import React, { useState, useEffect } from "react";
import "./Cards.css";
import RedditCard from "../SourceCard/RedditCard";
import TrustpilotCard from "../SourceCard/TrustpilotCard";
import YoutubeCard from "../SourceCard/YoutubeCard";
import { fetchRedditData, fetchTrustpilotData, fetchYoutubeData } from "../../Data/Data";

const SourceCards = ({ companyName }) => {
  const [redditCardData, setRedditCardData] = useState({});
  const [trustpilotCardData, setTrustpilotCardData] = useState({});
  const [youtubeCardData, setYoutubeCardData] = useState({});

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      setError(null);
      try {

        const reddit_data = await fetchRedditData(companyName);
        setRedditCardData(reddit_data || {});

        const trustpilot_data = await fetchTrustpilotData(companyName);
        setTrustpilotCardData(trustpilot_data || {});

        const youtube_data = await fetchYoutubeData(companyName);
        setYoutubeCardData(youtube_data || {});
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

  return (
    <div className="cards">
      {/* Reddit Card */}
      {Object.keys(redditCardData).length > 0 && (
        <RedditCard
          title="Reddit"
          data={redditCardData}
          color={{
            backGround: "#FBAF87",
            boxShadow: "0px 10px 20px 0px  #FBAF87",
            fill: "#F35607",
            stroke: "#F35607",
          }}
          icon={() => <span role="img" aria-label="Reddit icon">📘</span>}
        />
      )}

      {/* YouTube Card */}
      {Object.keys(youtubeCardData).length > 0 && (
        <YoutubeCard
          title="YouTube"
          data={youtubeCardData}
          color={{
            backGround: "#FB878E",
            boxShadow: "0px 10px 20px 0px #FB878E",
            fill: "#CB0715",
            stroke: "#CB0715",
          }}
          icon={() => <span role="img" aria-label="YouTube icon">🎥</span>}
        />
      )}

      {/* Trustpilot Card */}
      {Object.keys(trustpilotCardData).length > 0 && (
        <TrustpilotCard
          title="Trustpilot"
          data={trustpilotCardData}
          color={{
            backGround: "#78A9E0",
            boxShadow: "0px 10px 20px 0px #78A9E0",
            fill: "#0070C0",
            stroke: "#0070C0",
          }}
          icon={() => <span role="img" aria-label="Trustpilot icon">🌟</span>}
        />
      )}
    </div>
  );
};

export default SourceCards;
