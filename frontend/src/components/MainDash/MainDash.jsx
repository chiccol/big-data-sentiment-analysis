// MainDash.jsx
import React from "react";
import { useParams } from "react-router-dom"; // <-- import useParams
import Cards from "../Cards/Cards";
import SummaryTable from "../Table/Table";
import WordOccurrencesCards from "../WordOccurrencesCards/WordOccurrencesCards";
import "./MainDash.css";
// import SourceCards from "../SourceCards/SourceCards";

const MainDash = () => {
  const { company } = useParams(); 
  // 'company' will be "google" / "apple" / "nordvpn" / etc. 

  return (
    <div className="MainDash">
      <h1>Dashboard: {company || '... select a company on the left ...'}</h1> {/* Just to visualize */}
      {/* Pass the company param to the child components */}
      <Cards companyName={company} />
      <WordOccurrencesCards companyName={company} />
      {/* <SourceCards companyName={company} /> */}
      <SummaryTable companyName={company} />
    </div>
  );
};

export default MainDash;
