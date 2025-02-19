import subprocess

def get_installed_package_version(package_name):
    """パッケージのバージョンを取得"""
    try:
        result = subprocess.run(
            ["pip", "freeze"], capture_output=True, text=True, check=True
        )
        for line in result.stdout.splitlines():
            if line.startswith(package_name + "=="):
                return line.split("==")[1]
    except Exception as e:
        return f"Error: {e}"
    return "Not installed"

def get_package_dependencies(package_name):
    """パッケージの依存関係を取得"""
    try:
        result = subprocess.run(
            ["pip", "show", package_name], capture_output=True, text=True, check=True
        )
        for line in result.stdout.splitlines():
            if line.startswith("Requires:"):
                return line.split(":")[1].strip()
    except Exception as e:
        return f"Error: {e}"
    return "No dependencies found"

if __name__ == "__main__":
    websockets_version = get_installed_package_version("websockets")
    supabase_version = get_installed_package_version("supabase")
    supabase_dependencies = get_package_dependencies("supabase")

    print(f"Websockets Version: {websockets_version}")
    print(f"Supabase Version: {supabase_version}")
    print(f"Supabase Dependencies: {supabase_dependencies}")
