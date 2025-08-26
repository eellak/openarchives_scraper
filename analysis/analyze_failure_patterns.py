#!/usr/bin/env python3
"""Comprehensive analysis of failure patterns with multiple prediction models.

This script analyzes the failure rate patterns from scraper runs and provides:
1. Statistical analysis of failure trends
2. Multiple prediction models (linear, polynomial, exponential)
3. Confidence intervals for 800k run predictions
4. Visualization of actual vs predicted failure rates
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
from scipy.optimize import curve_fit
from sklearn.metrics import r2_score
import seaborn as sns

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.dpi'] = 150


def exponential_model(x, a, b, c):
    """Exponential growth model: y = a * exp(b * x) + c"""
    return a * np.exp(b * x) + c


def power_law_model(x, a, b, c):
    """Power law model: y = a * x^b + c"""
    return a * np.power(x, b) + c


def analyze_metrics(df: pd.DataFrame) -> dict:
    """Perform comprehensive statistical analysis of failure metrics."""
    
    # Basic statistics
    stats_dict = {
        'total_requests': int(df['completed'].max()),
        'total_failures': int(df['failed'].max()),
        'final_fail_pct': float(df['fail_pct'].iloc[-1]),
        'mean_fail_pct': float(df['fail_pct'].mean()),
        'std_fail_pct': float(df['fail_pct'].std()),
        'min_fail_pct': float(df['fail_pct'].min()),
        'max_fail_pct': float(df['fail_pct'].max()),
        'mean_speed_ps': float(df['speed_ps'].mean()),
    }
    
    # Trend analysis
    x = df['completed'].values
    y = df['fail_pct'].values
    
    # Remove any zero or negative values for log transforms
    mask = (x > 0) & (y > 0)
    x_clean = x[mask]
    y_clean = y[mask]
    
    # Linear regression
    slope, intercept, r_value, p_value, std_err = stats.linregress(x_clean, y_clean)
    stats_dict['linear_trend'] = {
        'slope': float(slope),
        'intercept': float(intercept),
        'r_squared': float(r_value**2),
        'p_value': float(p_value),
        'std_error': float(std_err)
    }
    
    # Calculate acceleration (second derivative)
    if len(df) > 2:
        derivatives = df['derivative_fail_pct_per_request'].values
        acceleration = np.diff(derivatives)
        stats_dict['acceleration'] = {
            'mean': float(np.mean(acceleration)),
            'std': float(np.std(acceleration)),
            'trend': 'accelerating' if np.mean(acceleration) > 0 else 'decelerating'
        }
    
    return stats_dict


def fit_models(df: pd.DataFrame, target_total: int = 830313):
    """Fit multiple prediction models and return predictions."""
    
    x = df['completed'].values
    y = df['fail_pct'].values
    
    # Normalize x for numerical stability
    x_norm = x / 1000.0
    target_norm = target_total / 1000.0
    
    models = {}
    
    # 1. Linear model (already in stats)
    slope, intercept, _, _, _ = stats.linregress(x, y)
    linear_pred = slope * target_total + intercept
    models['linear'] = {
        'prediction': float(linear_pred),
        'failed_count': int(linear_pred * target_total / 100)
    }
    
    # 2. Polynomial models
    for degree in [2, 3]:
        coeffs = np.polyfit(x_norm, y, degree)
        poly_func = np.poly1d(coeffs)
        y_pred = poly_func(x_norm)
        r2 = r2_score(y, y_pred)
        
        target_pred = float(poly_func(target_norm))
        models[f'polynomial_{degree}'] = {
            'prediction': target_pred,
            'failed_count': int(target_pred * target_total / 100),
            'r_squared': float(r2),
            'coefficients': coeffs.tolist()
        }
    
    # 3. Exponential model (with bounds to prevent overflow)
    try:
        popt, _ = curve_fit(exponential_model, x_norm, y, 
                          p0=[1, 0.001, 5],
                          bounds=([0, 0, 0], [100, 0.1, 100]),
                          maxfev=5000)
        y_pred_exp = exponential_model(x_norm, *popt)
        r2_exp = r2_score(y, y_pred_exp)
        
        exp_pred = float(exponential_model(target_norm, *popt))
        models['exponential'] = {
            'prediction': min(exp_pred, 100.0),  # Cap at 100%
            'failed_count': int(min(exp_pred, 100.0) * target_total / 100),
            'r_squared': float(r2_exp),
            'parameters': {'a': float(popt[0]), 'b': float(popt[1]), 'c': float(popt[2])}
        }
    except:
        models['exponential'] = {'prediction': None, 'error': 'Failed to converge'}
    
    # 4. Power law model
    try:
        # Shift y values to ensure positivity
        y_shifted = y - y.min() + 0.1
        popt_power, _ = curve_fit(power_law_model, x_norm, y_shifted,
                                p0=[1, 0.5, 0],
                                bounds=([0, 0, -10], [100, 2, 10]))
        y_pred_power = power_law_model(x_norm, *popt_power)
        # Shift back for r2 calculation
        y_pred_power_original = y_pred_power + y.min() - 0.1
        r2_power = r2_score(y, y_pred_power_original)
        
        power_pred = float(power_law_model(target_norm, *popt_power) + y.min() - 0.1)
        models['power_law'] = {
            'prediction': min(power_pred, 100.0),
            'failed_count': int(min(power_pred, 100.0) * target_total / 100),
            'r_squared': float(r2_power),
            'parameters': {'a': float(popt_power[0]), 'b': float(popt_power[1]), 'c': float(popt_power[2])}
        }
    except:
        models['power_law'] = {'prediction': None, 'error': 'Failed to converge'}
    
    # 5. Weighted ensemble prediction
    valid_predictions = []
    weights = []
    for name, model in models.items():
        if model.get('prediction') is not None and 'r_squared' in model:
            valid_predictions.append(model['prediction'])
            # Use R² as weight, with minimum weight of 0.1
            weights.append(max(model.get('r_squared', 0.5), 0.1))
    
    if valid_predictions:
        weights = np.array(weights) / sum(weights)
        ensemble_pred = np.average(valid_predictions, weights=weights)
        models['ensemble'] = {
            'prediction': float(ensemble_pred),
            'failed_count': int(ensemble_pred * target_total / 100),
            'components': len(valid_predictions),
            'weights': weights.tolist()
        }
    
    return models


def create_visualizations(df: pd.DataFrame, models: dict, output_dir: Path):
    """Create comprehensive visualizations of failure patterns and predictions."""
    
    # 1. Failure rate over time with fitted models
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    x = df['completed'].values
    y = df['fail_pct'].values
    
    # Plot actual data
    ax1.scatter(x, y, alpha=0.6, label='Actual', color='blue', s=30)
    
    # Plot fitted models
    x_extended = np.linspace(0, 830313, 1000)
    x_norm = x / 1000.0
    x_extended_norm = x_extended / 1000.0
    
    # Linear
    if 'linear' in models:
        slope = (y[-1] - y[0]) / (x[-1] - x[0])
        intercept = y[0] - slope * x[0]
        ax1.plot(x_extended, slope * x_extended + intercept, 
                label=f"Linear (pred: {models['linear']['prediction']:.1f}%)", 
                alpha=0.7, linestyle='--')
    
    # Polynomial
    if 'polynomial_3' in models and 'coefficients' in models['polynomial_3']:
        coeffs = models['polynomial_3']['coefficients']
        poly_func = np.poly1d(coeffs)
        y_poly = poly_func(x_extended_norm)
        ax1.plot(x_extended, y_poly, 
                label=f"Polynomial-3 (pred: {models['polynomial_3']['prediction']:.1f}%)", 
                alpha=0.7, linestyle='-.')
    
    # Exponential
    if 'exponential' in models and models['exponential'].get('prediction'):
        params = models['exponential']['parameters']
        y_exp = exponential_model(x_extended_norm, params['a'], params['b'], params['c'])
        ax1.plot(x_extended, np.minimum(y_exp, 100), 
                label=f"Exponential (pred: {models['exponential']['prediction']:.1f}%)", 
                alpha=0.7, linestyle=':')
    
    ax1.set_xlabel('Completed Requests')
    ax1.set_ylabel('Failure Rate (%)')
    ax1.set_title('Failure Rate Projections for 830k Requests')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(0, 850000)
    ax1.set_ylim(0, max(20, df['fail_pct'].max() * 1.5))
    
    # Add vertical line at current progress
    ax1.axvline(x[-1], color='red', linestyle='--', alpha=0.5, label=f'Current: {x[-1]}')
    
    # 2. Derivative analysis
    ax2.plot(df['completed'], df['derivative_fail_pct_per_request'], 
            label='Failure Rate Derivative', color='green', alpha=0.7)
    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    ax2.set_xlabel('Completed Requests')
    ax2.set_ylabel('d(Failure%)/d(Request)')
    ax2.set_title('Rate of Change in Failure Percentage')
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig(output_dir / 'failure_analysis_comprehensive.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 3. Model comparison bar chart
    fig, ax = plt.subplots(figsize=(10, 6))
    
    model_names = []
    predictions = []
    colors = []
    
    for name, model in models.items():
        if model.get('prediction') is not None:
            model_names.append(name.replace('_', ' ').title())
            predictions.append(model['prediction'])
            if name == 'ensemble':
                colors.append('darkred')
            else:
                colors.append('steelblue')
    
    bars = ax.bar(model_names, predictions, color=colors, alpha=0.7)
    
    # Add value labels on bars
    for bar, pred in zip(bars, predictions):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{pred:.1f}%\n({int(pred * 8303.13)}k fails)',
                ha='center', va='bottom')
    
    ax.set_ylabel('Predicted Failure Rate (%)')
    ax.set_title('Model Predictions for 830k Request Run')
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'model_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 4. Failure distribution histogram
    fig, ax = plt.subplots(figsize=(8, 6))
    
    ax.hist(df['fail_pct'], bins=20, alpha=0.7, color='coral', edgecolor='black')
    ax.axvline(df['fail_pct'].mean(), color='red', linestyle='--', 
              label=f'Mean: {df["fail_pct"].mean():.2f}%')
    ax.axvline(df['fail_pct'].iloc[-1], color='blue', linestyle='--', 
              label=f'Final: {df["fail_pct"].iloc[-1]:.2f}%')
    
    ax.set_xlabel('Failure Rate (%)')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of Failure Rates During Run')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'failure_distribution.png', dpi=150, bbox_inches='tight')
    plt.close()


def main():
    parser = argparse.ArgumentParser(description='Analyze failure patterns and predict 800k run outcomes')
    parser.add_argument('--metrics', nargs='+', required=True, 
                       help='Path(s) to metrics CSV files')
    parser.add_argument('--output-dir', default='analysis', 
                       help='Output directory for analysis results')
    parser.add_argument('--target-total', type=int, default=830313,
                       help='Target total requests for projection')
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load and combine all metrics files
    all_dfs = []
    for metrics_file in args.metrics:
        df = pd.read_csv(metrics_file)
        all_dfs.append(df)
    
    # Analyze each run separately and combined
    results = {
        'individual_runs': [],
        'combined_analysis': None,
        'recommendations': []
    }
    
    for i, df in enumerate(all_dfs):
        print(f"\nAnalyzing run {i+1} ({args.metrics[i]})...")
        
        # Statistical analysis
        stats = analyze_metrics(df)
        
        # Model fitting
        models = fit_models(df, args.target_total)
        
        # Create visualizations
        run_output_dir = output_dir / f'run_{i+1}'
        run_output_dir.mkdir(exist_ok=True)
        create_visualizations(df, models, run_output_dir)
        
        results['individual_runs'].append({
            'file': args.metrics[i],
            'statistics': stats,
            'models': models
        })
    
    # Combined analysis if multiple runs
    if len(all_dfs) > 1:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        combined_df = combined_df.sort_values('completed').reset_index(drop=True)
        
        print("\nAnalyzing combined data...")
        combined_stats = analyze_metrics(combined_df)
        combined_models = fit_models(combined_df, args.target_total)
        create_visualizations(combined_df, combined_models, output_dir / 'combined')
        
        results['combined_analysis'] = {
            'statistics': combined_stats,
            'models': combined_models
        }
    
    # Generate recommendations
    all_predictions = []
    for run in results['individual_runs']:
        for model_name, model in run['models'].items():
            if model.get('prediction') is not None:
                all_predictions.append(model['prediction'])
    
    if all_predictions:
        mean_pred = np.mean(all_predictions)
        std_pred = np.std(all_predictions)
        
        results['recommendations'] = [
            f"Average prediction across all models: {mean_pred:.1f}% ± {std_pred:.1f}%",
            f"Expected failures for 830k run: {int(mean_pred * 8303.13)}k - {int((mean_pred + std_pred) * 8303.13)}k",
            f"Confidence interval (95%): {mean_pred - 1.96*std_pred:.1f}% - {mean_pred + 1.96*std_pred:.1f}%"
        ]
        
        if mean_pred > 10:
            results['recommendations'].append(
                "HIGH FAILURE RATE: Consider implementing retry logic or increasing timeouts"
            )
        
        # Check for acceleration
        accelerating_runs = sum(1 for run in results['individual_runs'] 
                              if run['statistics'].get('acceleration', {}).get('trend') == 'accelerating')
        if accelerating_runs > len(results['individual_runs']) / 2:
            results['recommendations'].append(
                "ACCELERATING FAILURES: Failure rate is increasing non-linearly. "
                "Server may implement progressive rate limiting."
            )
    
    # Save results
    with open(output_dir / 'analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    print("\n" + "="*60)
    print("ANALYSIS SUMMARY")
    print("="*60)
    
    for i, run in enumerate(results['individual_runs']):
        print(f"\nRun {i+1}:")
        print(f"  Total requests: {run['statistics']['total_requests']}")
        print(f"  Final failure rate: {run['statistics']['final_fail_pct']:.2f}%")
        print(f"  Model predictions for 830k:")
        for model_name, model in run['models'].items():
            if model.get('prediction') is not None:
                print(f"    {model_name}: {model['prediction']:.1f}% ({model['failed_count']:,} failures)")
    
    print("\n" + "-"*60)
    print("RECOMMENDATIONS:")
    for rec in results['recommendations']:
        print(f"  • {rec}")
    
    print(f"\nFull results saved to: {output_dir / 'analysis_results.json'}")
    print(f"Visualizations saved to: {output_dir}")


if __name__ == '__main__':
    main()