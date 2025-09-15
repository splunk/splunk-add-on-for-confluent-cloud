import os
from os import path

def additional_packaging(addon_name: str) -> None:
    print(f"🔧 Building UI for addon: {addon_name}")
    
    current_dir = os.path.dirname(os.path.realpath(__file__))
    build_ui_script = os.path.join(current_dir, "scripts", "build-ui.sh")
    
    if path.exists(build_ui_script):
        print(f"📦 Executing UI build script: {build_ui_script}")
        os.system(f"chmod +x {build_ui_script}")
        return_code = os.system(build_ui_script)
        
        if return_code != 0:
            print(f"❌ UI build failed with return code: {return_code}")
            os._exit(os.WEXITSTATUS(return_code))
        else:
            print("✅ UI build completed successfully")
    else:
        print(f"⚠️ Build script not found: {build_ui_script}")
