import psutil

# Function to get system information
def get_system_resources():
    # CPU Usage
    cpu_usage = psutil.cpu_percent(interval=1)
    
    # Memory Usage
    memory = psutil.virtual_memory()
    memory_usage = memory.percent
    
    # Disk Usage
    disk = psutil.disk_usage('/')
    disk_usage = disk.percent

    return cpu_usage, memory_usage, disk_usage

# Function to provide upgrade recommendations
def get_recommendation(cpu_usage, memory_usage, disk_usage):
    recommendations = []
    
    # Check if CPU usage is too high
    if cpu_usage > 85:
        recommendations.append("CPU usage is high. Consider upgrading to a faster processor.")
    
    # Check if memory usage is too high
    if memory_usage > 85:
        recommendations.append("Memory usage is high. Consider upgrading your RAM.")
    
    # Check if disk usage is too high
    if disk_usage > 85:
        recommendations.append("Disk space is running low. Consider upgrading your storage or cleaning up unnecessary files.")
    
    # If everything is under control
    if not recommendations:
        recommendations.append("Your system resources are well-optimized.")
    
    return recommendations

def main():
    # Get the system resources usage
    cpu_usage, memory_usage, disk_usage = get_system_resources()
    
    # Print current system resources
    print(f"CPU Usage: {cpu_usage}%")
    print(f"Memory Usage: {memory_usage}%")
    print(f"Disk Usage: {disk_usage}%")
    
    # Get and print the recommendation based on usage
    recommendations = get_recommendation(cpu_usage, memory_usage, disk_usage)
    print("\nRecommendations:")
    for rec in recommendations:
        print(f"- {rec}")

if __name__ == "__main__":
    main()
