import pickle
import numpy as np
import pandas as pd
import os
from typing import Dict, List, Any
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from .stop_to_stop_data import StopToStopDataProcessor


class UniversalStopTimeModel:
    def __init__(self, model_dir: str = "universal_models"):
        self.model_dir = model_dir
        self.model = None
        self.scaler = None
        os.makedirs(model_dir, exist_ok=True)

    def train(self, vehicle_positions_path: str, stops_data_path: str):
        """Train universal stop-to-stop model"""
        print("Starting universal model training...")

        # Process stop-to-stop data
        processor = StopToStopDataProcessor(vehicle_positions_path, stops_data_path)
        stop_pairs = processor.process_all_routes()

        if len(stop_pairs) < 10:
            print("Insufficient stop-to-stop data for training")
            return

        # Extract features and targets
        print("Extracting features...")
        X_all = []
        y_all = []

        for pair in stop_pairs:
            features = processor.extract_features(pair)
            travel_time = pair['travel_time']

            X_all.append(features)
            y_all.append(travel_time)

        X = np.array(X_all)
        y = np.array(y_all)

        print(f"Training with {len(X)} stop-to-stop samples")
        print(f"Feature dimension: {X.shape[1]}")

        # Scale features
        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X)

        # Train model using median prediction (quantile regression approach)
        # We'll use a custom prediction method that computes median from tree predictions
        from sklearn.ensemble import RandomForestRegressor

        self.model = RandomForestRegressor(
            n_estimators=200,
            max_depth=15,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1
        )
        self.model.fit(X_scaled, y)

        # Store original model for median predictions
        self._use_median = True

        print("Universal model training completed!")
        print("Model configured to use MEDIAN predictions for more accurate estimates")

    def save_model(self):
        """Save the universal model and scaler"""
        if self.model is None or self.scaler is None:
            print("No model to save")
            return

        model_path = os.path.join(self.model_dir, "universal_model.pkl")
        scaler_path = os.path.join(self.model_dir, "universal_scaler.pkl")

        with open(model_path, 'wb') as f:
            pickle.dump(self.model, f)

        with open(scaler_path, 'wb') as f:
            pickle.dump(self.scaler, f)

        print(f"Universal model saved to {self.model_dir}")

    def load_model(self):
        """Load the universal model and scaler"""
        model_path = os.path.join(self.model_dir, "universal_model.pkl")
        scaler_path = os.path.join(self.model_dir, "universal_scaler.pkl")

        if os.path.exists(model_path) and os.path.exists(scaler_path):
            with open(model_path, 'rb') as f:
                self.model = pickle.load(f)

            with open(scaler_path, 'rb') as f:
                self.scaler = pickle.load(f)

            print("Universal model loaded successfully")
            return True
        else:
            print("Universal model files not found")
            return False

    def predict_median(self, X_scaled: np.ndarray) -> np.ndarray:
        """
        Predict using median of tree predictions instead of mean.
        This provides more robust predictions, especially when dealing with outliers.

        For stop-to-stop times: If historical data shows 5-15 minutes range,
        median will give you ~10 minutes instead of potentially higher mean values.
        """
        if self.model is None:
            raise ValueError("Model not loaded")

        # Get predictions from all individual trees
        tree_predictions = np.array([
            tree.predict(X_scaled) for tree in self.model.estimators_
        ])

        # Compute median across trees for each sample
        # This gives the 50th percentile prediction
        median_predictions = np.median(tree_predictions, axis=0)

        return median_predictions

    def predict_mode(self, X_scaled: np.ndarray, n_bins: int = 50) -> np.ndarray:
        """
        Predict using mode (most frequent value) of tree predictions.
        This provides the most common prediction, which often represents typical conditions.

        For stop-to-stop times: If historical data shows most trips take 8-9 minutes
        during peak hours with occasional delays to 15+ minutes, mode will give you
        the most frequent value (~8-9 min) rather than mean (~11 min) or median (~10 min).

        Args:
            X_scaled: Scaled feature array
            n_bins: Number of bins for discretizing predictions to find mode

        Returns:
            Mode predictions for each sample
        """
        if self.model is None:
            raise ValueError("Model not loaded")

        # Get predictions from all individual trees
        tree_predictions = np.array([
            tree.predict(X_scaled) for tree in self.model.estimators_
        ])

        # For each sample, find the mode of tree predictions
        mode_predictions = []

        for sample_idx in range(tree_predictions.shape[1]):
            sample_preds = tree_predictions[:, sample_idx]

            # Bin the predictions to find mode
            # (continuous values need binning to find "most frequent")
            min_val = sample_preds.min()
            max_val = sample_preds.max()

            if max_val - min_val < 1e-6:  # All predictions are the same
                mode_predictions.append(sample_preds[0])
                continue

            # Create bins
            bins = np.linspace(min_val, max_val, n_bins + 1)
            hist, bin_edges = np.histogram(sample_preds, bins=bins)

            # Find the bin with maximum count
            mode_bin_idx = np.argmax(hist)

            # Use the center of the mode bin as the prediction
            mode_value = (bin_edges[mode_bin_idx] + bin_edges[mode_bin_idx + 1]) / 2

            mode_predictions.append(mode_value)

        return np.array(mode_predictions)