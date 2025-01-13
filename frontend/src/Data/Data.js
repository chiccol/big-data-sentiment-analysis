// Data.js
export const getCompaniesData = async () => {
  try {
    const response = await fetch("http://localhost:8000/companies");
    if (!response.ok) {
      throw new Error("Failed to fetch companies data");
    }
    const data = await response.json();
    // data should be of the form { companies: [...] }
    return data.companies;
  } catch (error) {
    console.error(error);
    return [];
  }
};

export const fetchDashboardData = async (companyName) => {
  try {
    // Fetch "Main" data
    const mainResponse = await fetch(
      `http://localhost:8000/super-aggregated-postgres-data/${companyName}`
    );
    if (!mainResponse.ok) {
      throw new Error("Failed to fetch main data");
    }
    const mainData = await mainResponse.json();

    // Fetch "Reddit", "Trustpilot", and "YouTube" data
    const platformsResponse = await fetch(
      `http://localhost:8000/aggregated-postgres-data/${companyName}`
    );
    if (!platformsResponse.ok) {
      throw new Error("Failed to fetch platform data");
    }
    const platformsData = await platformsResponse.json();

    // 1) Extract "aggregated_data" for main
    const aggregatedMain = mainData.aggregated_data || [];
    const main = aggregatedMain.map((entry) => ({
      date: entry.date,
      score: entry.score,
    }));

    // 2) Extract & process the "aggregated_data" for reddit, trustpilot, youtube
    const aggregatedPlatforms = platformsData.aggregated_data || [];

    const reddit = aggregatedPlatforms
      .filter((item) => item.reddit !== null && item.reddit !== undefined)
      .map((item) => ({
        date: item.date,
        score: item.reddit,
      }));

    const trustpilot = aggregatedPlatforms
      .filter((item) => item.trustpilot !== null && item.trustpilot !== undefined)
      .map((item) => ({
        date: item.date,
        score: item.trustpilot,
      }));

    const youtube = aggregatedPlatforms
      .filter((item) => item.youtube !== null && item.youtube !== undefined)
      .map((item) => ({
        date: item.date,
        score: item.youtube,
      }));

    // Return all card data
    return {
      main,
      reddit,
      trustpilot,
      youtube,
    };
  } catch (error) {
    console.error("Error fetching dashboard data:", error);
    throw error;
  }
};

/**
 * Fetch top words for the given company.
 * Returns data in the format: [ { word: "...", occurrences: number }, ... ]
 */
export const fetchTopWords = async (companyName) => {
  try {
    const response = await fetch(`http://localhost:8000/top_words/${companyName}`);
    if (!response.ok) {
      throw new Error("Failed to fetch top words");
    }
    const data = await response.json(); 
    // data looks like: { company: "nordvpn", top_words: [ { word: "internet", count: 5 }, ...] }

    // Transform for WordOccurrenceCard
    const wordsData = data.top_words.map(({ word, count }) => ({
      word,
      occurrences: count,
    }));
    return wordsData;
  } catch (error) {
    console.error("Error fetching top words data:", error);
    return [];
  }
};

/**
 * Fetch top bigrams for the given company.
 * Returns data in the format: [ { word: "some bigram", occurrences: number }, ... ]
 */
export const fetchTopBigrams = async (companyName) => {
  try {
    const response = await fetch(`http://localhost:8000/top_couples/${companyName}`);
    if (!response.ok) {
      throw new Error("Failed to fetch top bigrams");
    }
    const data = await response.json();
    // data looks like: { company: "nordvpn", top_bigrams: [ { bigram: "internet service", count: 5 }, ... ] }

    const bigramsData = data.top_bigrams.map(({ bigram, count }) => ({
      word: bigram,
      occurrences: count,
    }));
    return bigramsData;
  } catch (error) {
    console.error("Error fetching top bigrams data:", error);
    return [];
  }
};

/**
 * Fetch top trigrams for the given company.
 * Returns data in the format: [ { word: "some trigram", occurrences: number }, ... ]
 */
export const fetchTopTrigrams = async (companyName) => {
  try {
    const response = await fetch(`http://localhost:8000/top_triples/${companyName}`);
    if (!response.ok) {
      throw new Error("Failed to fetch top trigrams");
    }
    const data = await response.json();
    // data looks like: { company: "nordvpn", top_trigrams: [ { trigram: "something about internet", count: 3 }, ... ] }

    const trigramsData = data.top_trigrams.map(({ trigram, count }) => ({
      word: trigram,
      occurrences: count,
    }));
    return trigramsData;
  } catch (error) {
    console.error("Error fetching top trigrams data:", error);
    return [];
  }
};

export const fetchLastComments = async (companyName) => {
  try {
    const response = await fetch(`http://localhost:8000/last_comment/${companyName}`);
    if (!response.ok) {
      throw new Error("Failed to fetch last comments");
    }
    const data = await response.json(); 
    // data => { reddit: '...', trustpilot: '...', youtube: '...' }
    return data;
  } catch (error) {
    console.error("Error fetching last comments:", error);
    // Return empty or default comments if an error occurs
    return {
      reddit: "",
      trustpilot: "",
      youtube: "",
    };
  }
};
